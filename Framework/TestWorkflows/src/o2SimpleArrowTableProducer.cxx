// Copyright CERN and copyright holders of ALICE O2. This software is
// distributed under the terms of the GNU General Public License v3 (GPL
// Version 3), copied verbatim in the file "COPYING".
//
// See http://alice-o2.web.cern.ch/license for full licensing information.
//
// In applying this license CERN does not waive the privileges and immunities
// granted to it by virtue of its status as an Intergovernmental Organization
// or submit itself to any jurisdiction.

#include "Framework/ConfigParamSpec.h"
#include "FairLogger.h"
#include <lzma.h>
#include <lz4.h>
#include <chrono>
#include <list>
#include <iomanip>
#include <arrow/builder.h>
#include <arrow/array.h>
#include <arrow/compute/kernel.h>
#include <arrow/compute/context.h>
#include <arrow/io/memory.h>
#include <parquet/arrow/writer.h>
#include <parquet/arrow/reader.h>

using namespace o2::framework;

// This has to be declared before including runDataProcessing.h
void customize(std::vector<ConfigParamSpec>& opt) {
  opt.push_back({ "sleep", VariantType::Int, 1,
                  {"Sleep (secs) at each producer iteration"} });
  opt.push_back({ "how-many", VariantType::Int, 5000000,
                  {"How many int to generate per iteration"} });
  opt.push_back({ "how-many-smear", VariantType::Int, 1000000,
                  {"Maximum divergency over the ints generated"} });
  opt.push_back({ "stop-at-iter", VariantType::Int, -1,
                  {"Stop at iterations (-1 == don't stop)"} });
}

#include "Framework/runDataProcessing.h"
#include "Framework/ParallelContext.h"
#include "Framework/ControlService.h"

#define ARROW_DPL_HANDLE(x) { \
  arrow::Status _x = (x); \
  if (!_x.ok()) { \
    LOG(ERROR) << "Arrow runtime problem: " << _x.ToString() << FairLogger::endl; \
    assert(false); \
  } \
}

WorkflowSpec defineDataProcessing(ConfigContext const &config) {

  WorkflowSpec w;

  auto sleepSec = config.options().get<int>("sleep");
  auto howMany = config.options().get<int>("how-many");
  auto howManySmear = config.options().get<int>("how-many-smear");
  auto stopAtIterations = config.options().get<int>("stop-at-iter");

  // Data producer: we produce chunks of random data
  w.push_back({
    "ParquetProd",
    Inputs{},
    { OutputSpec{"TST", "PQT"} },
    AlgorithmSpec{
      AlgorithmSpec::InitCallback{
        [sleepSec, howMany, howManySmear, stopAtIterations](InitContext &setup) {

          auto iterCount = std::make_shared<int>(0);
          auto askedToQuit = std::make_shared<bool>(false);

          return [iterCount, askedToQuit, sleepSec, howMany, howManySmear, stopAtIterations](ProcessingContext &ctx) {

            if (sleepSec >= 0) {
              sleep(sleepSec);
            }
            if (*askedToQuit) return;

            // Apache Arrow test here. We produce random data and we write it using the Parquet
            // format. Interesting references:
            // https://arrow.apache.org/docs/cpp/md_tutorials_row_wise_conversion.html

            // A note on memory usage. The Arrow Builder has its own memory pool, this can be
            // optimized of course to avoid growing it over and over. Arrow data is NOT copied to
            // the DPL buffers: instead, we create an arbitrarily large buffer where Parquet (NOT
            // Arrow) data is stored. DPL will dispose of it when appropriate. One downside is that
            // the Parquet TableWriter does not handle buffer overruns at the moment, and
            // this is quite dangerous.
            // TODO: Stream columns independently, as different DPL channels, reassemble it in local
            // tables according to the subscribers.

            int howManyNow = howMany + (rand() % (2 * howManySmear) - howManySmear);

            // Column 1
            arrow::Int32Builder builderCol1;
            for (int i=0; i<howManyNow; i++) {
              ARROW_DPL_HANDLE(builderCol1.Append( rand() % 1000 ));
            }
            std::shared_ptr<arrow::Array> col1;
            ARROW_DPL_HANDLE(builderCol1.Finish(&col1));

            // Column 2
            arrow::Int32Builder builderCol2;
            for (int i=0; i<howManyNow; i++) {
              ARROW_DPL_HANDLE(builderCol2.Append( rand() % 1000 + 1000 ));
            }
            std::shared_ptr<arrow::Array> col2;
            ARROW_DPL_HANDLE(builderCol2.Finish(&col2));

            // Define a schema: two int32 columns
            std::vector<std::shared_ptr<arrow::Field>> schema_vector = {
                arrow::field("col1", arrow::int32()),
                arrow::field("col2", arrow::int32())
            };
            auto schema = std::make_shared<arrow::Schema>(schema_vector);

            // Construct a table: schema + columns
            std::shared_ptr<arrow::Table> table = arrow::Table::Make(schema, {col1, col2});

            // Now, this part of the test has ended. We should dump our Table memory onto somewhere
            // that can be easily streamed by the DPL.

            // First, we create a buffer which is large enough. We use allocators (Mikolaj) in order
            // to tell DPL to dispose of it properly at the end. Buffer size is estimated
            auto alloc = boost::container::pmr::new_delete_resource();
            size_t outputBufSize = sizeof(int32_t) * schema_vector.size() * (howManyNow+10);
            LOG(INFO) << "Creating buffer of " << outputBufSize << " bytes for "
                      << howManyNow << " rows" << FairLogger::endl;
            uint8_t *rawOutputBuf = static_cast<uint8_t*>(alloc->allocate(outputBufSize, alignof(std::max_align_t)));

            // Our Arrow buffer nicely wraps the memory we've created for it. It needs to be a
            // MutableBuffer otherwise we cannot write to it.
            // Note! We make room for 8 more bytes at the beginning, hosting the number of bytes
            // effectively written. This is essential.
            auto arrowOutputBuf = std::make_shared<arrow::MutableBuffer>(&rawOutputBuf[8], outputBufSize-8);

            // Now we create a FixedSizeBufferWriter backed by the Arrow buffer
            auto arrowOutputStream = std::make_shared<arrow::io::FixedSizeBufferWriter>(arrowOutputBuf);

            // The output file format is Parquet. Parquet needs a buffer to write data to
            int64_t pos;
            ARROW_DPL_HANDLE(arrowOutputStream->Tell(&pos));
            assert(pos == 0);
            ARROW_DPL_HANDLE(parquet::arrow::WriteTable(*table,
                                                        arrow::default_memory_pool(),
                                                        arrowOutputStream,
                                                        10000));
            ARROW_DPL_HANDLE(arrowOutputStream->Tell(&pos));

            LOG(INFO) << "Written " << pos << " bytes in Parquet format" << FairLogger::endl;

            // Write data size
            memcpy(rawOutputBuf, &pos, 8);

            // Now we have a DPL buffer filled with Parquet stuff. Let's stream it over the network.
            // Note that we stream ALL of it even if we've filled PART of it only
            ctx.outputs().adoptChunk(Output{"TST", "PQT"},
                                     reinterpret_cast<char *>(rawOutputBuf), outputBufSize,
                                     o2::header::Stack::getFreefn(), alloc);

            (*iterCount)++;
            if (stopAtIterations > 0 && *iterCount >= stopAtIterations) {
              LOG(WARN) << "Producer has reached " << (*iterCount)
                        << " iterations: stopping" << FairLogger::endl;
              *askedToQuit = true;
              ctx.services().get<ControlService>().readyToQuit(true);
            }
          };
        }
      }
    }
  });

  // Printer: receives Parquet data, spits it out
  w.push_back({
    "Printer",
    { InputSpec{"parquetData", "TST", "PQT"} },
    {},
    AlgorithmSpec{
      AlgorithmSpec::InitCallback{
        [](InitContext &setup) {
          // ...init...
          return [](ProcessingContext &ctx) {

            auto inp = ctx.inputs().get("parquetData");
            auto rawData = DataRefUtils::as<uint8_t>(inp);  // gives a span of uint8_t: we can read the size

            // The first 8 bytes are reserved to store the actual stream portion used. This is
            // essential for Parquet as it looks at the end of the stream first
            auto actualSize = DataRefUtils::as<int64_t>(inp)[0];

            // Deserialize Parquet data to Arrow.
            // See https://github.com/apache/parquet-cpp/blob/master/examples/parquet-arrow/src/reader-writer.cc

            // Parquet file reader. It does not necessarily read from file (we want to read a buffer)
            std::unique_ptr<parquet::arrow::FileReader> parquetReader;

            // Here is our buffer. It is not mutable. We create it from gsl::span
            auto arrowInputBuf = std::make_shared<arrow::MutableBuffer>(&(rawData.data())[8], actualSize);

            // We now need a buffer reader that reads it
            auto arrowInputStream = std::make_shared<arrow::io::BufferReader>(arrowInputBuf);

            // And now we tell Parquet to use the given buffer reader
            ARROW_DPL_HANDLE(parquet::arrow::OpenFile(arrowInputStream,
                                                      arrow::default_memory_pool(),
                                                      &parquetReader));

            // Now we read the whole table (note that there are many other options, including
            // reading single row groups or columns)
            std::shared_ptr<arrow::Table> table;
            ARROW_DPL_HANDLE(parquetReader->ReadTable(&table));

            // Print out some stats. We need the table schema for the column names
            std::stringstream buf;
            auto schema = table->schema();
            for (int i=0; i<schema->num_fields(); i++) {
              if (buf.tellp()) buf << ", ";
              buf << schema->field(i)->name();
            }
            LOG(INFO) << "Loaded rows: " << table->num_rows()
                      << ", columns: " << table->num_columns()
                      << " (" << buf.str() << ")" << FairLogger::endl;

            // Print an excerpt of the actual data
            for (auto colName : { "col1", "col2" }) {
              auto idx = schema->GetFieldIndex(colName);
              assert(idx >= 0);
              auto readCol = table->column(idx);  // type: Column
              auto readAry = std::static_pointer_cast<arrow::Int32Array>(readCol->data()->chunk(0));  // type: Array
              for (int i=0; i<table->num_rows() && i<5; i++) {
                LOG(INFO) << "Excerpt: " << colName << ": position " << i
                          << ": " << readAry->Value(i) << FairLogger::endl;
              }
            }

          };
        }
      }
    }
  });

  return w;
}

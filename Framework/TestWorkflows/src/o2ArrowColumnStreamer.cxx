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
#include <boost/tokenizer.hpp>

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
  opt.push_back({ "tasks", VariantType::String, "Task1=col1+col2,Task2=col1,Task3=col2",
                  {"Tasks to execute"} });
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
  auto tasks = config.options().get<std::string>("tasks");

  std::set<std::string> dynColumnNames;
  std::map<std::string, std::set<std::string>> dynTasksCols;
  int numChannels = 0;

  // Parse tasks definition and columns, store results in dyn* variables
  boost::tokenizer<boost::char_separator<char>> tokTasks{ tasks, boost::char_separator<char>{","} };
  for (auto t : tokTasks) {
    boost::tokenizer<boost::char_separator<char>> tokDef{ t, boost::char_separator<char>{"="} };
    assert(std::distance(tokDef.begin(), tokDef.end()) == 2);
    auto tokDefIt = tokDef.begin();
    boost::tokenizer<boost::char_separator<char>> tokCols{ *(++tokDefIt), boost::char_separator<char>{"+"} };
    std::set<std::string> x;
    for (auto c : tokCols) {
      dynColumnNames.insert(c);
      x.insert(c);
    }
    dynTasksCols.insert(std::make_pair(*(tokDef.begin()), std::move(x)));
  }

  // It seems there is no way (and that's deliberate) to generate a DataDescription on the fly, its
  // length is validated at compile time
  const std::vector<o2::header::DataDescription> dataDesc =
    { "PQ1", "PQ2", "PQ3", "PQ4", "PQ5", "PQ6", "PQ7", "PQ8", "PQ9" };

  // Define output specs for the data producer/reader: one channel per column per task
  auto outputSpecs = Outputs{};
  for (auto p : dynTasksCols) {
    LOG(info) << "Input data for " << p.first << ":" << FairLogger::endl;
    for (auto c : p.second) {
      LOG(info) << "  " << c << FairLogger::endl;
      outputSpecs.push_back( OutputSpec{"TST", dataDesc[numChannels++]} );
    }
  }

  LOG(ERROR) << "Exiting on purpose" << FairLogger::endl;
  assert(false);
  // -------------------------------------------------------------------------------------------- //

  constexpr int numColumns = 2;


  // Data producer: we produce chunks of random data
  w.push_back({
    "ParquetProd",
    Inputs{},
    outputSpecs,
    AlgorithmSpec{
      AlgorithmSpec::InitCallback{
        [sleepSec, howMany, howManySmear, stopAtIterations, dataDesc](InitContext &setup) {

          auto iterCount = std::make_shared<int>(0);
          auto askedToQuit = std::make_shared<bool>(false);

          return [iterCount, askedToQuit, sleepSec, howMany, howManySmear, stopAtIterations, dataDesc](ProcessingContext &ctx) {

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

            // Out of convenience we keep our Array columns in a vector
            std::vector<std::shared_ptr<arrow::Array>> columns = { col1, col2 };

            // For each Array we create a buffer that the DPL will adopt
            auto alloc = boost::container::pmr::new_delete_resource();
            size_t outputBufSize = sizeof(int32_t) * (howManyNow+10);
            int count = 0;
            for (auto c : columns) {
              LOG(INFO) << "Creating buffer of " << outputBufSize << " bytes for "
                        << howManyNow << " rows" << FairLogger::endl;
              uint8_t *rawOutputBuf = static_cast<uint8_t*>(alloc->allocate(outputBufSize, alignof(std::max_align_t)));
              auto arrowOutputBuf = std::make_shared<arrow::MutableBuffer>(&rawOutputBuf[8], outputBufSize-8);
              auto arrowOutputStream = std::make_shared<arrow::io::FixedSizeBufferWriter>(arrowOutputBuf);

              // We create one-column-wide tables; Parquet seems to like Tables, not Arrays
              std::vector<std::shared_ptr<arrow::Field>> schema_vector = {
                  arrow::field("colTODO_NAME", arrow::int32())
              };
              auto schema = std::make_shared<arrow::Schema>(schema_vector);
              std::shared_ptr<arrow::Table> table = arrow::Table::Make(schema, { columns[count] });

              long long pos;
              ARROW_DPL_HANDLE(arrowOutputStream->Tell(&pos));
              assert(pos == 0);
              ARROW_DPL_HANDLE(parquet::arrow::WriteTable(*table,
                                                          arrow::default_memory_pool(),
                                                          arrowOutputStream,
                                                          10000));
              ARROW_DPL_HANDLE(arrowOutputStream->Tell(&pos));

              // Write size (used in deserializing as DPL does not allow for input truncation ATM)
              memcpy(rawOutputBuf, &pos, 8);

              // DPL adopts our memory and will dispose of it properly
              ctx.outputs().adoptChunk(Output{"TST", dataDesc[count]},
                                       reinterpret_cast<char *>(rawOutputBuf), outputBufSize,
                                       o2::header::Stack::getFreefn(), alloc);

              LOG(INFO) << "Written " << pos << " bytes in Parquet format" << FairLogger::endl;
              count++;
            }

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

  // Define printers here. They have the same boilerplate inside, so let's do it
  // once.
  std::map<std::string, Inputs> inputDefs = {
    { "Printer1", { InputSpec{"pqd1", "TST", "PQ1"} } },
    { "Printer2", { InputSpec{"pqd2", "TST", "PQ2"} } },
    { "Printer3", { InputSpec{"pqd1", "TST", "PQ1"},
                    InputSpec{"pqd2", "TST", "PQ2"} } }
  };

  // Printer: receives Parquet data, spits it out
  w.push_back({
    "Printer1",
    { InputSpec{"pqd1", "TST", "PQ1"} },
    {},
    AlgorithmSpec{
      AlgorithmSpec::InitCallback{
        [](InitContext &setup) {
          // ...init...
          return [](ProcessingContext &ctx) {
          };
        }
      }
    }
  });

  w.push_back({
    "Printer2",
    { InputSpec{"pqd2", "TST", "PQ2"} },
    {},
    AlgorithmSpec{
      AlgorithmSpec::InitCallback{
        [](InitContext &setup) {
          // ...init...
          return [](ProcessingContext &ctx) {
            // ...processing...
          };
        }
      }
    }
  });

  w.push_back({
    "Printer3",
    {
      InputSpec{"pqd1", "TST", "PQ1"},
      InputSpec{"pqd2", "TST", "PQ2"}
    },
    {},
    AlgorithmSpec{
      AlgorithmSpec::InitCallback{
        [](InitContext &setup) {
          // ...init...
          return [](ProcessingContext &ctx) {
            // ...processing...
          };
        }
      }
    }
  });

  return w;
}

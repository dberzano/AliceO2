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
#include <ROOT/TDataFrame.hxx>
#include <ROOT/TArrowDS.hxx>

using namespace o2::framework;
using namespace ROOT::Experimental;

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
  opt.push_back({ "tasks", VariantType::String, "Task1=col1+col2,Task2=col1,Task3=col2,Task4=col1+col2+col3+col4",
                  {"Tasks to execute"} });
}

#include "Framework/runDataProcessing.h"
#include "Framework/ParallelContext.h"
#include "Framework/ControlService.h"

#define ARROW_DPL_HANDLE(x) { \
  arrow::Status _x = (x); \
  if (!_x.ok()) { \
    LOG(error) << "Arrow runtime problem: " << _x.ToString() << FairLogger::endl; \
    assert(false); \
  } \
}

o2::header::DataDescription getPqDataDesc(int idx) {
  o2::header::DataDescription dd;
  std::string s = "PQ" + std::to_string(idx);
  dd.runtimeInit(s.c_str());
  return dd;
}

template <typename T>
std::stringstream catAry(const T &iterable) {
  std::stringstream buf;
  for (auto i : iterable) {
    if (buf.tellp()) buf << ", ";
    buf << i;
  }
  return buf;
}

std::stringstream catLabels(ProcessingContext &ctx) {
  std::list<const char *> labels;
  for (auto i : ctx.inputs()) {
    labels.push_back((i.spec)->binding.c_str());
  }
  return catAry(labels);
}

WorkflowSpec defineDataProcessing(ConfigContext const &config) {

  WorkflowSpec w;

  auto sleepSec = config.options().get<int>("sleep");
  auto howMany = config.options().get<int>("how-many");
  auto howManySmear = config.options().get<int>("how-many-smear");
  auto stopAtIterations = config.options().get<int>("stop-at-iter");
  auto tasks = config.options().get<std::string>("tasks");

  auto dynColumnNames = std::make_shared<std::set<std::string>>();
  auto dynTasksCols = std::make_shared<std::map<std::string, std::set<std::string>>>();
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
      dynColumnNames->insert(c);
      x.insert(c);
    }
    dynTasksCols->insert(std::make_pair(*(tokDef.begin()), std::move(x)));
  }

  // Define output specs for the data producer/reader: one channel per column per task
  auto outputSpecs = Outputs{};
  for (auto p : *dynTasksCols) {
    LOG(info) << "Input data for " << p.first << ":" << FairLogger::endl;
    for (auto c : p.second) {
      LOG(info) << "  " << c << " (#" << numChannels << ")" << FairLogger::endl;
      outputSpecs.push_back( OutputSpec{"TST", getPqDataDesc(numChannels++)} );
    }
  }

  // Define the producer device
  auto srcProducer = DataProcessorSpec{
    "ParquetProd",
    Inputs{},
    outputSpecs,
    AlgorithmSpec{
      AlgorithmSpec::InitCallback{
        [sleepSec, howMany, howManySmear, stopAtIterations,
         dynColumnNames, dynTasksCols](InitContext &setup) {

          auto iterCount = std::make_shared<int>(0);
          auto askedToQuit = std::make_shared<bool>(false);

          return [iterCount, askedToQuit, sleepSec, howMany, howManySmear, stopAtIterations,
                  dynColumnNames, dynTasksCols](ProcessingContext &ctx) {

            if (sleepSec >= 0) {
              sleep(sleepSec);
            }
            if (*askedToQuit) return;

            // Start of Parquet data producer

            // Apache Arrow test here. We produce random data and we write it using the Parquet
            // format. Interesting references:
            // https://arrow.apache.org/docs/cpp/md_tutorials_row_wise_conversion.html

            // A note on memory usage. The Arrow Builder has its own memory pool, this can be
            // optimized of course to avoid growing it over and over. Arrow data is NOT copied to
            // the DPL buffers: instead, we create an arbitrarily large buffer where Parquet (NOT
            // Arrow) data is stored. DPL will dispose of it when appropriate. One downside is that
            // the Parquet TableWriter does not handle buffer overruns at the moment, and
            // this is quite dangerous.

            // Determine how much data to produce dynamically
            int howManyNow = howMany + (rand() % (2 * howManySmear) - howManySmear);

            // Now produce random data. For simplicity, we only have int32 as a type
            int numCol = 0;
            for (auto c : *dynColumnNames) {
              LOG(info) << "Producing " << howManyNow << " entries for " << c << FairLogger::endl;
              auto builder = std::make_shared<arrow::Int32Builder>();
              for (int i=0; i<howManyNow; i++) {
                ARROW_DPL_HANDLE(builder->Append(rand() % 1000 + numCol * 1000));
              }
              numCol++;
              std::shared_ptr<arrow::Array> array;
              ARROW_DPL_HANDLE(builder->Finish(&array));

              // For each Array, we create a one-column-wide table for Parquet (TODO: maybe there is
              // a better way that does not imply reverse-engineering the Arrow memory format?!)
              auto schema_vec = std::vector<std::shared_ptr<arrow::Field>>{
                arrow::field(c, arrow::int32())  // proper column name and type
              };
              auto schema = std::make_shared<arrow::Schema>(schema_vec);
              auto table = arrow::Table::Make(schema, {array});  // a single column

              // We need to write our data someplace. We create a PoolBuffer, memory is managed by
              // Arrow itself
              // Note: use MutableBuffer for memory managed by us as buffer, and use
              // FixedSizeBufferWriter as stream. No boundary check would occur in that case!
              auto buffer = std::make_shared<arrow::PoolBuffer>(arrow::default_memory_pool());
              auto stream = std::make_shared<arrow::io::BufferOutputStream>(buffer);

              // We now write; first we make sure the stream works as expected
              long long bytesWritten = 12345;
              ARROW_DPL_HANDLE(stream->Tell(&bytesWritten));
              assert(bytesWritten == 0);

              // Write table in Parquet format
              ARROW_DPL_HANDLE(parquet::arrow::WriteTable(*table,
                                                          arrow::default_memory_pool(),
                                                          stream,
                                                          10000));

              // Check how many bytes were written and the buffer size
              ARROW_DPL_HANDLE(stream->Tell(&bytesWritten));
              LOG(info) << "Written " << bytesWritten << " bytes (Parquet) for " << c
                        << ", buffer is " << buffer->size() << " bytes" << FairLogger::endl;

              // We have to decide where to stream this column now
              // TODO: this lookup is horrible and should be done elsewhere once for all, use a LUT
              int colIdx = 0;
              for (auto p : *dynTasksCols) {
                auto thisIt = p.second.find(c);
                if (thisIt != p.second.end()) {
                  int thisIdx = std::distance(p.second.begin(), thisIt) + colIdx;
                  LOG(info) << "Column " << c << " is needed by task " << p.first
                            << " with index " << thisIdx
                            << FairLogger::endl;

                  // Snapshot data for the DPL. We need allocation magic. TODO: this is less than
                  // optimal, from a memory perspective
                  // TODO: verify whether it makes sense to copy only bytesWritten, maybe the buffer
                  // size is what we want, due to the fact that Parquet writes meta information at
                  // the end of the stream (or the buffer?)
                  auto alloc = boost::container::pmr::new_delete_resource();
                  uint8_t *rawDplBuf = static_cast<uint8_t*>(alloc->allocate(bytesWritten, alignof(std::max_align_t)));
                  std::copy(buffer->data(), buffer->data()+bytesWritten, rawDplBuf);
                  ctx.outputs().adoptChunk(Output{"TST", getPqDataDesc(thisIdx)},
                                           reinterpret_cast<char *>(rawDplBuf), bytesWritten,
                                           o2::header::Stack::getFreefn(), alloc);

                }
                colIdx += p.second.size();
              }
            }

            (*iterCount)++;
            if (stopAtIterations > 0 && *iterCount >= stopAtIterations) {
              LOG(warn) << "Producer has reached " << (*iterCount)
                        << " iterations: stopping" << FairLogger::endl;
              *askedToQuit = true;
              ctx.services().get<ControlService>().readyToQuit(true);
            }

            // End of Parquet data producer

          };
        }
      }
    }
  };

  w.push_back(std::move(srcProducer));

  // Add dummy tasks (TODO: we can improve and use external functions)
  numChannels = 0;
  for (auto p : *dynTasksCols) {
    auto inputSpecs = Inputs{};
    for (auto c : p.second) {
      inputSpecs.push_back( InputSpec{c, "TST", getPqDataDesc(numChannels)} );
      numChannels++;
    }

    // Here, we add the task
    w.push_back({
      p.first,  // task name as specified on the command line
      inputSpecs,
      {},
      AlgorithmSpec{
        AlgorithmSpec::InitCallback{
          [](InitContext &setup) {
            
            // Initialization: just enable ImplicitMT
            constexpr int numThreads = 3;
            ROOT::EnableImplicitMT(numThreads);

            return [](ProcessingContext &ctx) {

              LOG(info) << "I have columns " << catLabels(ctx).str() << FairLogger::endl;

              // Empty table
              //auto schema = std::make_shared<arrow::Schema>(std::vector<std::shared_ptr<arrow::Field>>{});
              //auto table = arrow::Table::Make(schema, std::vector<std::shared_ptr<arrow::Array>>{});
              std::shared_ptr<arrow::Table> table;  // lazily inited

              // Have to assemble several inputs into a Table. Single input channels are Tables
              // themselves.
              std::vector<std::string> columnNames;
              for (auto input : ctx.inputs()) {
                LOG(info) << "Getting column " << (input.spec)->binding << FairLogger::endl;
                columnNames.push_back((input.spec)->binding);
                auto rawDplBuf = DataRefUtils::as<uint8_t>(input);  // gsl::span --> data(), size()

                // Parquet + Arrow boilerplate for reading
                std::unique_ptr<parquet::arrow::FileReader> reader;
                auto buffer = std::make_shared<arrow::MutableBuffer>(rawDplBuf.data(), rawDplBuf.size());
                auto stream = std::make_shared<arrow::io::BufferReader>(buffer);
                ARROW_DPL_HANDLE(parquet::arrow::OpenFile(stream,
                                                          arrow::default_memory_pool(),
                                                          &reader));
                std::shared_ptr<arrow::Table> tmpTable;
                ARROW_DPL_HANDLE(reader->ReadTable(&tmpTable));

                // Add the only column to the existing joined table
                if (table.get() == nullptr) {
                  table = tmpTable;
                }
                else {
                  // AddColumn check if the number of rows match
                  ARROW_DPL_HANDLE(table->AddColumn(table->num_columns(), tmpTable->column(0), &table));
                }
              }

              auto printStats = [&table]() {
                // Now print stats of the produced table
                assert(table.get() != nullptr);
                std::stringstream buf;
                auto schema = table->schema();
                for (int i=0; i<schema->num_fields(); i++) {
                  if (buf.tellp()) buf << ", ";
                  buf << schema->field(i)->name();
                }
                LOG(info) << "Loaded rows: " << table->num_rows()
                          << ", columns: " << table->num_columns()
                          << " (" << buf.str() << ")" << FairLogger::endl;

                // Print an excerpt of the actual data
                for (int i=0; i<table->num_columns(); i++) {
                  auto column = table->column(i);
                  auto array = std::static_pointer_cast<arrow::Int32Array>(column->data()->chunk(0));
                  buf.str(std::string());
                  buf.clear();
                  for (int j=0; j<table->num_rows() && j<5; j++) {
                    if (buf.tellp()) buf << ", ";
                    buf << std::to_string(array->Value(j));
                  }
                  LOG(info) << "Excerpt of Column " << schema->field(i)->name()
                            << ": " << buf.str() << FairLogger::endl;
                }
              };
              //printStats();

              auto runDataFrame = [&table, &columnNames]() {
                // This is the actual "analysis timeframe loop", seeing only data it subscribed to
                TDataFrame tdf = TDF::MakeArrowDataFrame(table, columnNames);
                // TODO we work on the first column only (until we find a good way to process
                // multiple columns)
                std::array<double,numThreads> partialSums{};
                tdf.ForeachSlot([&partialSums](int slot, int32_t n) {
                  partialSums[slot] += n;
                }, {columnNames[0]});
                double sum = 0.;
                for (auto p : partialSums) {
                  sum += p;
                }
                double avg = sum / (double)table->num_rows();
                LOG(info) << "Average of Column " << columnNames[0]
                         << " using TDF: " << avg << FairLogger::endl;
              };
              runDataFrame();

            };
          }
        }
      }
    });
  }

  return w;
}

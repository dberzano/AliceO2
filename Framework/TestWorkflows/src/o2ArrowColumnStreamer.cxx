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

  auto dynColumnNames = std::make_shared<std::set<std::string>>();
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
      dynColumnNames->insert(c);
      x.insert(c);
    }
    dynTasksCols.insert(std::make_pair(*(tokDef.begin()), std::move(x)));
  }

  // It seems there is no way (and that's deliberate) to generate a DataDescription on the fly, its
  // length is validated at compile time
  const std::vector<o2::header::DataDescription> dataDesc =
    { "PQ0", "PQ1", "PQ2", "PQ3", "PQ4", "PQ5", "PQ6", "PQ7", "PQ8", "PQ9" };

  // Define output specs for the data producer/reader: one channel per column per task
  auto outputSpecs = Outputs{};
  for (auto p : dynTasksCols) {
    LOG(info) << "Input data for " << p.first << ":" << FairLogger::endl;
    for (auto c : p.second) {
      LOG(info) << "  " << c << FairLogger::endl;
      outputSpecs.push_back( OutputSpec{"TST", dataDesc[numChannels++]} );
    }
  }

  // Define the producer device
  auto srcProducer = DataProcessorSpec{
    "ParquetProd",
    Inputs{},
    outputSpecs,
    AlgorithmSpec{
      AlgorithmSpec::InitCallback{
        [sleepSec, howMany, howManySmear, stopAtIterations, dataDesc,
         dynColumnNames](InitContext &setup) {

          auto iterCount = std::make_shared<int>(0);
          auto askedToQuit = std::make_shared<bool>(false);

          return [iterCount, askedToQuit, sleepSec, howMany, howManySmear, stopAtIterations,
                  dataDesc, dynColumnNames](ProcessingContext &ctx) {

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
            std::vector<std::shared_ptr<arrow::Int32Builder>> arrowBuilders;
            std::vector<std::shared_ptr<arrow::Array>> arrowArrays;
            std::vector<std::shared_ptr<arrow::Table>> arrowTables;
            for (auto c : *dynColumnNames) {
              LOG(info) << "Producing " << howManyNow << " entries for " << c << FairLogger::endl;
              auto builder = std::make_shared<arrow::Int32Builder>();
              for (int i=0; i<howManyNow; i++) {
                ARROW_DPL_HANDLE(builder->Append(rand() % 1000 + i * 1000));
              }
              std::shared_ptr<arrow::Array> array;
              ARROW_DPL_HANDLE(builder->Finish(&array));
              arrowBuilders.push_back(builder);  // XXX
              arrowArrays.push_back(array);  // XXX

              // For each Array, we create a one-column-wide table for Parquet (TODO: maybe there is
              // a better way that does not imply reverse-engineering the Arrow memory format?!)
              auto schema_vec = std::vector<std::shared_ptr<arrow::Field>>{
                arrow::field(c, arrow::int32())  // proper column name and type
              };
              auto schema = std::make_shared<arrow::Schema>(schema_vec);
              auto table = arrow::Table::Make(schema, {array});  // a single column
              arrowTables.push_back(table);  // XXX

              // We need to write our data someplace. We create a PoolBuffer, memory is managed by
              // Arrow itself
              // Note: use MutableBuffer for memory managed by us as buffer, and use
              // FixedSizeBufferWriter as stream. No boundary check would occur in that case!
              auto buffer = std::make_shared<arrow::PoolBuffer>(arrow::default_memory_pool());
              auto stream = std::make_shared<arrow::io::BufferOutputStream>(buffer);

              // We now write; first we make sure the stream works as expected
              long long pos = 12345;
              ARROW_DPL_HANDLE(stream->Tell(&pos));
              assert(pos == 0);

              // Write table in Parquet format
              ARROW_DPL_HANDLE(parquet::arrow::WriteTable(*table,
                                                          arrow::default_memory_pool(),
                                                          stream,
                                                          10000));

              // Check how many bytes were written and the buffer size
              ARROW_DPL_HANDLE(stream->Tell(&pos));
              LOG(info) << "Written " << pos << " bytes (Parquet) for " << c
                        << ", buffer is " << buffer->size() << " bytes" << FairLogger::endl;

            }

            (*iterCount)++;
            if (stopAtIterations > 0 && *iterCount >= stopAtIterations) {
              LOG(WARN) << "Producer has reached " << (*iterCount)
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

  constexpr int numColumns = 2;

  // Add dummy tasks (TODO: we can improve and use external functions)
  numChannels = 0;
  for (auto p : dynTasksCols) {
    auto inputSpecs = Inputs{};
    for (auto c : p.second) {
      std::string x = "parquet" + std::to_string(numChannels);
      inputSpecs.push_back( InputSpec{x, "TST", dataDesc[numChannels++]} );
    }

    // Here, we add the task
    w.push_back({
      p.first,  // task name as specified on the command line
      inputSpecs,
      {},
      AlgorithmSpec{
        AlgorithmSpec::InitCallback{
          [](InitContext &setup) {
            // ...init...
            return [](ProcessingContext &ctx) {
              LOG(info) << "This is a task doing nothing" << FairLogger::endl;
            };
          }
        }
      }
    });
  }

  return w;
}

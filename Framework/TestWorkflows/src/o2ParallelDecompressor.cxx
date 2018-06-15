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
#include <lz4.h>
#include <chrono>
#include <list>
#include <iomanip>

using namespace o2::framework;

// Make our life easier in measuring times...
typedef std::chrono::time_point<std::chrono::high_resolution_clock> TimePoint_t;
inline TimePoint_t now() {
  return std::chrono::high_resolution_clock::now();
};

// This has to be declared before including runDataProcessing.h
void customize(std::vector<ConfigParamSpec>& opt) {
  opt.push_back({ "num-unzippers", VariantType::Int, 1,
                  {"Number of decompressors"} });
  opt.push_back({ "num-workers", VariantType::Int, 1,
                  {"Number of workers processing decompressed data"} });
  opt.push_back({ "sleep", VariantType::Int, 0,
                  {"Sleep (secs) at each reader iteration"} });
  opt.push_back({ "loop", VariantType::Bool, false,
                  {"Run continuously"} });
  opt.push_back({ "infiles", VariantType::String, "",
                  {"Output base file name"} });
  opt.push_back({ "num-files", VariantType::Int, 1,
                  {"Number of input files"} });
  opt.push_back({ "cat-only", VariantType::Bool, false,
                  {"Just cat, do not decompress (speed test)"} });
}

#include "Framework/runDataProcessing.h"
#include "Framework/ParallelContext.h"
#include "Framework/ControlService.h"

template <typename T>
std::stringstream catAry(const T &iterable) {
  std::stringstream buf;
  for (auto i : iterable) {
    if (buf.tellp()) buf << ", ";
    buf << i;
  }
  return buf;
}

WorkflowSpec defineDataProcessing(ConfigContext const &config) {

  WorkflowSpec w;

  auto numUnzippers = config.options().get<int>("num-unzippers");
  auto numWorkers = config.options().get<int>("num-workers");
  auto sleepSec = config.options().get<int>("sleep");
  auto loop = config.options().get<bool>("loop");
  auto infiles = config.options().get<std::string>("infiles");
  auto nfiles = config.options().get<int>("num-files");
  auto catOnly = config.options().get<bool>("cat-only");

  // Input file reader: reads many files in sequence
  w.push_back({
    "Reader",
    Inputs{},
    { OutputSpec{"TST", "CMP"} },
    AlgorithmSpec{
      AlgorithmSpec::InitCallback{
        [sleepSec, loop, infiles, nfiles](InitContext &setup) {
          auto iterCount = std::make_shared<int>(0);
          auto askedToQuit = std::make_shared<bool>(false);
          std::vector<std::string> inputList;
          for (int i=1; i<=nfiles; i++) {
            // List of dummy input files
            std::string s = infiles + "_" + std::to_string(i);
            inputList.push_back(s);
          }

          class InputFileContext {
            private:
              std::vector<std::string> mInputList;
              int mCurFile;
              std::ifstream mStream;
              bool mLoop;
            public:
              InputFileContext(std::vector<std::string> &&inputList, bool loop)
                : mCurFile(-1), mInputList(inputList), mLoop(loop) {
                next();
              };
              bool next() {
                if (++mCurFile == mInputList.size() && mLoop) {
                  mCurFile = 0;  // circular
                }
                if (mStream.is_open()) {
                  mStream.close();
                  mStream.clear();
                }
                if (mCurFile == mInputList.size() && !mLoop) {
                  return false;
                }
                mStream.open(mInputList[mCurFile], std::fstream::binary);
                return true;
              };
              inline std::ifstream &get() { return mStream; }
              inline std::string &get_name() { return mInputList[mCurFile]; }
          };

          auto shCtx = std::make_shared<InputFileContext>(std::move(inputList), loop);

          // ...more init boilerplate...

          return [iterCount, askedToQuit, sleepSec, shCtx](ProcessingContext &ctx) {

            if (*askedToQuit) return;

            auto& fCtx = *shCtx; 

            if (sleepSec > 0) {
              LOG(INFO) << "Reader is sleeping..." << FairLogger::endl;
              sleep(sleepSec);
            }

            // Check stream status
            if (fCtx.get().fail()) {
              if (!fCtx.next()) {
                LOG(INFO) << "No more files: asking to quit" << FairLogger::endl;
                *askedToQuit = true;
                ctx.services().get<ControlService>().readyToQuit(true);
              }
            }

            // Read next chunk from file
            unsigned int chunkSize = 0;  // chunk size in file
            fCtx.get().read((char *)&chunkSize, 4);
            if (fCtx.get().fail()) return;

            unsigned int uncompSize = 0;  // size of chunk when uncompressed (hint for mallocing)
            fCtx.get().read((char *)&uncompSize, 4);
            if (fCtx.get().fail()) return;

            // We know the chunk size: allocate memory and read compressed data
            // We send the uncompressed size as the first 4 bytes of the chunk
            auto alloc = boost::container::pmr::new_delete_resource();
            uint8_t *compData = static_cast<uint8_t*>(alloc->allocate(chunkSize+4, alignof(std::max_align_t)));
            memcpy(compData, &uncompSize, 4);  // first 4 bytes: uncompressed size
            fCtx.get().read((char *)&compData[4], chunkSize);  // rest: compressed data
            if (fCtx.get().fail()) {
              LOG(ERROR) << "Cannot read expected " << chunkSize
                         << " bytes from " << fCtx.get_name() << FairLogger::endl;
              alloc->deallocate(compData, chunkSize);
              assert(false);  // this should not happen in our example
              return;
            }

            // In the meanwhile adopt allocated chunk (pmr way)
            LOG(INFO) << "Chunk read: compressed size is " << chunkSize
                      << ", uncompressed size is " << uncompSize
                      << ", now sending" << FairLogger::endl;
            ctx.outputs().adoptChunk(Output{"TST", "CMP"},
                                     reinterpret_cast<char *>(compData), chunkSize+4,
                                     o2::header::Stack::getFreefn(), alloc);

            // DEBUG: increment loop counter
            (*iterCount)++;

          };
        }
      }
    }
  });

  // Parallel decompressors FTW
  w.push_back(timePipeline(
    {
      "Decompressor",
      { InputSpec{{"compData"}, "TST", "CMP"} },
      { OutputSpec{"TST", "DAT"} },
      AlgorithmSpec{

        AlgorithmSpec::InitCallback{
          [catOnly](InitContext &setup) {
            // ...init...
            return [catOnly](ProcessingContext &ctx) {

              // Read compressed blob
              auto inp = ctx.inputs().get("compData");
              auto compData = DataRefUtils::as<char>(inp).data();
              auto chunkSize = DataRefUtils::as<char>(inp).size();

              // chunkSize is (4 + zipped payload), in the first 4 bytes we have the uncompressed size
              unsigned int uncompSize;
              memcpy(&uncompSize, compData, 4);
              compData = &compData[4];
              chunkSize -= 4;

              LOG(INFO) << "Decompressor received " << chunkSize << " compressed bytes "
                        << "(uncompressed: " << uncompSize << ")" << FairLogger::endl;

              // Ask DPL to give us a buffer for uncompressed data. We ask for 4 more bytes: we
              // write the original compressed size there for reference
              auto uncompData = ctx.outputs().make<char>({"TST", "DAT"}, uncompSize+4);

              // Store original compressed size for reference
              memcpy(&uncompData[0], &chunkSize, 4);

              // Decompress
              if (!catOnly) {
                int actualUncompSize = LZ4_decompress_safe(compData, &uncompData[4], chunkSize, uncompSize);
                assert(actualUncompSize == uncompSize);  // input dataset is sane
              }

            };
          }
        }
      }
    },
    numUnzippers
  ));

  // Debug: single worker that receives uncompressed data and writes on screen. We need this to
  // estimate the processing ratio
  w.push_back(timePipeline(
    {
      "Printer",
      { InputSpec{"uncompData", "TST", "DAT"} },
      {},
      AlgorithmSpec{
        AlgorithmSpec::InitCallback{
          [](InitContext &setup) {

            // Keep stats of time elapsed and processed compressed bytes.
            // Do it in an encapsulated way.
            class statSpeed {
              private:
                float size; // MB
                TimePoint_t t0;
                float speed;  // MB/s
                unsigned long calls;
              public:
                statSpeed() : size(0.), t0(now()), speed(0.), calls(0) {};
                float update(float addSize) {
                  calls++;
                  size += addSize;
                  auto msec = std::chrono::duration_cast<std::chrono::milliseconds>(now() - t0).count();
                  speed = 1000. * size / msec;
                  return speed;
                }
                inline auto getSpeed() { return speed; }
                inline auto getSize() { return size; }
                inline auto isTimeToPrint() { return (calls % 100) == 0; }
            };

            auto stats = std::make_shared<statSpeed>();

            return [stats](ProcessingContext &ctx) {
              auto inp = ctx.inputs().get("uncompData");
              auto uncompData = DataRefUtils::as<int>(inp);  // gives a span of int
              auto compSize = uncompData[0];
              uncompData = uncompData.subspan(1, uncompData.size()-1);

              // LOG(INFO) << "Processed compressed size of " << compSize << " bytes, "
              //           << "printing excerpt (5 of " << uncompData.size() << "): "
              //           << catAry(gsl::span<int>(uncompData.data(), 5)).str() << FairLogger::endl;

              stats->update((float)compSize/1000000.);  //  bytes to MB
              if (stats->isTimeToPrint()) {
                LOG(INFO) << "Data processed: " << stats->getSize() << " MB, "
                          << "rate: " << stats->getSpeed() << " MB/s"
                          << FairLogger::endl;
              }

            };
          }
        }
      }
    },
    numWorkers
  ));

  return w;
}

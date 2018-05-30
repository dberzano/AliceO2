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

using namespace o2::framework;

// This has to be declared before including runDataProcessing.h
void customize(std::vector<ConfigParamSpec>& opt) {
  opt.push_back({ "num-zippers", VariantType::Int, 1,
                  {"Number of compressors"} });
  opt.push_back({ "sleep", VariantType::Int, 0,
                  {"Sleep (secs) at each producer iteration"} });
  opt.push_back({ "stop-at-iter", VariantType::Int, -1,
                  {"Stop at iterations (-1 == don't stop)"} });
  opt.push_back({ "stop-at-stable", VariantType::Bool, false,
                  {"Stop when speed is stable"} });
  opt.push_back({ "how-many", VariantType::Int, 5000000,
                  {"How many int to generate per iteration"} });
  opt.push_back({ "algo", VariantType::String, "lzma",
                  {"What compression algo to use (lzma, lz4, cat)"} });
  opt.push_back({ "outfile", VariantType::String, "/dev/null",
                  {"Output file name"} });
  opt.push_back({ "rand", VariantType::Bool, false,
                  {"Generate random data at each iteration (slows down)"} });
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

std::stringstream catLabels(ProcessingContext &ctx) {
  std::list<const char *> labels;
  for (auto i : ctx.inputs()) {
    labels.push_back((i.spec)->binding.c_str());
  }
  return catAry(labels);
}

void defaultAmend(DataProcessorSpec &spec, size_t idx) {
  LOG(WARN) << "Using default amender for device " << spec.name << FairLogger::endl;
  for (auto &i : spec.inputs) {
    i.subSpec = idx;
  }
  for (auto &o : spec.outputs) {
    o.subSpec = idx;
  }
}

size_t cat(const uint8_t *inbuf, uint8_t *outdata, size_t sz, size_t outSz) {
  memcpy(outdata, inbuf, sz);
  return sz;
}

size_t lzmaCompress(const uint8_t *inbuf, uint8_t *outdata, size_t sz, size_t outSz) {
  lzma_stream lzs = LZMA_STREAM_INIT;
  lzma_action lza = LZMA_RUN;
  lzma_ret ret = lzma_easy_encoder(&lzs, 9, LZMA_CHECK_NONE);  // xz -9
  assert(ret == LZMA_OK);

  // Mini output buffer (will dump on outdata when full/finished)
  uint8_t outbuf[BUFSIZ];

  // Init LZMA state machine
  lzs.next_in = nullptr;
  lzs.avail_in = 0;
  lzs.next_out = outbuf;
  lzs.avail_out = sizeof(outbuf);

  // Total bytes read
  size_t nRead = 0;
  size_t nWritten = 0;

  while (true) {

    if (lzs.avail_in == 0 && nRead < sz) {
      // Fill input buffer if empty
      lzs.next_in = &inbuf[nRead];
      lzs.avail_in = sz-nRead; // all remaining bytes
      if (lzs.avail_in > BUFSIZ) lzs.avail_in = BUFSIZ; // truncate at BUFSIZ
      nRead += lzs.avail_in;
      if (nRead == sz) lza = LZMA_FINISH;
    }
    ret = lzma_code(&lzs, lza);
    if (lzs.avail_out == 0 || ret == LZMA_STREAM_END) {
      // We need to empty the output data
      size_t nToWrite = sizeof(outbuf) - lzs.avail_out;
      memcpy(&outdata[nWritten], outbuf, nToWrite);
      nWritten += nToWrite;

      // Reset output reference
      lzs.next_out = outbuf;
      lzs.avail_out = sizeof(outbuf);
    }

    if (ret != LZMA_OK) {
      assert(ret == LZMA_STREAM_END);
      break;
    }
  }
  lzma_end(&lzs);
  return nWritten;
}

inline size_t lz4Compress(const uint8_t *inbuf, uint8_t *outdata, size_t sz, size_t outSz) {
  return (size_t)LZ4_compress_default((const char *)inbuf, (char *)outdata, (int)sz, (int)outSz);
}

WorkflowSpec defineDataProcessing(ConfigContext const &config) {

  WorkflowSpec w;

  auto numZippers = config.options().get<int>("num-zippers");
  auto sleepSec = config.options().get<int>("sleep");
  auto stopAtIterations = config.options().get<int>("stop-at-iter");
  auto stopAtStable = config.options().get<bool>("stop-at-stable");
  auto howMany = config.options().get<int>("how-many");
  auto algo = config.options().get<std::string>("algo");
  auto outfile = config.options().get<std::string>("outfile");
  auto doRand = config.options().get<bool>("rand");

  std::function<size_t(const uint8_t *inbuf, uint8_t *outdata, size_t inSz, size_t outSz)> compressFn = nullptr;

  if (algo == "lzma") {
    compressFn = lzmaCompress;
  }
  else if (algo == "lz4") {
    compressFn = lz4Compress;
  }
  else {
    assert(algo == "cat");
    compressFn = cat;
  }

  // Data producer: we produce chunks of random data
  w.push_back({
    "Producer",
    Inputs{},
    { OutputSpec{"TST", "ARR"} },
    AlgorithmSpec{
      AlgorithmSpec::InitCallback{
        [sleepSec, howMany, stopAtIterations, doRand](InitContext &setup) {
          auto iterCount = std::make_shared<int>(0);
          auto askedToQuit = std::make_shared<bool>(false);
          auto randomBuf = std::make_shared<std::vector<int>>(howMany);

          if (!doRand) {
            // Initialize random buffer once for all
            for (int i=0; i<howMany; i++) {
              (*randomBuf)[i] = rand() % 1000;
            }
          }

          return [iterCount, askedToQuit, randomBuf, sleepSec, howMany, stopAtIterations, doRand](ProcessingContext &ctx) {

            if (*askedToQuit) return;

            if (doRand) {
              // Generate new random data at each iteration
              for (int i=0; i<howMany; i++) {
                (*randomBuf)[i] = rand() % 1000;
              }
            }

            if (sleepSec > 0) {
              LOG(INFO) << "Producer is sleeping..." << FairLogger::endl;
              sleep(sleepSec);
            }

            // Send random data to the single output (parallelization occurs over time)
            ctx.outputs().snapshot(Output{"TST", "ARR"}, (*randomBuf));
            LOG(INFO) << "Producer is sending data" << FairLogger::endl;
            ++(*iterCount);

            if (stopAtIterations > 0 && *iterCount >= stopAtIterations) {
              LOG(WARN) << "Reached " << (*iterCount) << " iterations: stopping" << FairLogger::endl;
              *askedToQuit = true;
              ctx.services().get<ControlService>().readyToQuit(true);
            }
          };
        }
      }
    }
  });

  // Parallel compressors
  w.push_back(timePipeline(
    {
      "Compressor",
      { InputSpec{{"arrCons"}, "TST", "ARR"} },
      { OutputSpec{"TST", "ZIP"},
        OutputSpec{"TST", "SIZ"} },
      AlgorithmSpec{

        AlgorithmSpec::InitCallback{
          [compressFn](InitContext &setup) {
            return [compressFn](ProcessingContext &ctx) {

              auto inp = ctx.inputs().get("arrCons");
              auto arrowCons = DataRefUtils::as<int>(inp);
              auto arrowBytes = (o2::header::get<o2::header::DataHeader*>(inp.header))->payloadSize;
              LOG(INFO) << "Compressor received " << arrowBytes << " bytes " << FairLogger::endl;

              // We create a buffer where to place our data, and compress there
              size_t outbufSz = arrowBytes * 2;
              auto alloc = boost::container::pmr::new_delete_resource();
              uint8_t *outbuf = static_cast<uint8_t*>(alloc->allocate(outbufSz, alignof(std::max_align_t)));
              size_t nWritten = compressFn((const uint8_t *)(&arrowCons[0]), outbuf, arrowBytes, outbufSz);

              // Send out the original size for reference
              auto origSz = ctx.outputs().make<size_t>(Output{"TST", "SIZ"}, 1);
              origSz[0] = arrowBytes;

              // Framework adopts our memory chunk and disposes of it
              ctx.outputs().adoptChunk(Output{"TST", "ZIP"},
                                       reinterpret_cast<char *>(outbuf), nWritten,
                                       o2::header::Stack::getFreefn(), alloc);
            };
          }
        }
      }
    },
    numZippers
  ));

  // Single writer
  w.push_back({
    "Writer",
    { InputSpec{"zipCons", "TST", "ZIP"},
      InputSpec{"origSize", "TST", "SIZ"} },
    {},
    AlgorithmSpec{
      AlgorithmSpec::InitCallback{
        [outfile, stopAtStable](InitContext &setup) {
          auto fout = std::make_shared<std::fstream>(outfile, std::fstream::out);
          if ((*fout).fail()) {
            std::stringstream msg;
            msg << "Cannot open file " << outfile << " for writing!";
            throw std::runtime_error(msg.str());
          }
          auto t0 = std::chrono::high_resolution_clock::now();
          auto countBytes = std::make_shared<size_t>(0);
          auto lastTime = std::make_shared<std::chrono::time_point<std::chrono::high_resolution_clock>>(t0);
          auto lastSpeed = std::make_shared<float>(0);
          return [stopAtStable, fout, t0, countBytes, lastTime, lastSpeed](ProcessingContext &ctx) {
            auto rawZip = ctx.inputs().get("zipCons");
            auto zipMsg = DataRefUtils::as<char>(rawZip);
            auto origSize = ctx.inputs().get<size_t>("origSize");
            *countBytes += zipMsg.size();
            auto tNow = std::chrono::high_resolution_clock::now();
            auto tDiff = std::chrono::duration_cast<std::chrono::milliseconds>(tNow - t0).count();
            float rate = (float)*countBytes / (float)tDiff / 1000.;

            LOG(INFO) << "Written " << (*countBytes/1000000) << " MB in "
                      << tDiff << " ms, that is " << rate << " MB/s" << FairLogger::endl;
            LOG(INFO) << "Writer has received " << zipMsg.size() << " bytes (originally " << origSize
                      << ", ratio is " << std::setprecision(3) << ((float)zipMsg.size()/(float)origSize)
                      <<"), now writing to disk" << FairLogger::endl;

            // Benchmark. Stop if writing speed stopped oscillating
            auto tDiffLast = std::chrono::duration_cast<std::chrono::seconds>(tNow - *lastTime).count();
            if (tDiffLast > 10) {
              LOG(INFO) << "Time elapsed" << FairLogger::endl;
              auto err = std::abs(*lastSpeed - rate) / *lastSpeed;
              LOG(INFO) << "Speed changed " << (err*100) << " percent" << FairLogger::endl;
              if (stopAtStable && err < 0.1) {
                LOG(WARN) << "Benchmark purposes reached: requesting stop" << FairLogger::endl;
                ctx.services().get<ControlService>().readyToQuit(true);
              }
              *lastTime = std::chrono::high_resolution_clock::now();
              *lastSpeed = rate;
            }

            // We keep the format very simple:
            // * 4 bytes: this size
            // * 4 bytes: uncompressed size
            // * N bytes: data (with N = this size)
            auto zipSz = zipMsg.size();
            (*fout).write((const char *)(&zipSz), 4);
            (*fout).write((const char *)(&origSize), 4);
            (*fout).write(zipMsg.data(), zipMsg.size());

          };
        }
      }
    }
  });

  return w;
}

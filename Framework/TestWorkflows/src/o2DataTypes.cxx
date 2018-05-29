// Copyright CERN and copyright holders of ALICE O2. This software is
// distributed under the terms of the GNU General Public License v3 (GPL
// Version 3), copied verbatim in the file "COPYING".
//
// See http://alice-o2.web.cern.ch/license for full licensing information.
//
// In applying this license CERN does not waive the privileges and immunities
// granted to it by virtue of its status as an Intergovernmental Organization
// or submit itself to any jurisdiction.

#include "Framework/runDataProcessing.h"

using namespace o2::framework;

struct PairOfNumbers {
  int a;
  double b;
};

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

WorkflowSpec defineDataProcessing(ConfigContext const&specs) {
  return WorkflowSpec{

    {
      "Producer",
      Inputs{},
      {
        OutputSpec{{"intProd_1"}, "TST", "A1_1"},
        OutputSpec{{"intProd_2"}, "TST", "A1_2"},
        OutputSpec{{"intAryProd"}, "TST", "A2"},
        OutputSpec{{"stringProd"}, "TST", "A3"},
        OutputSpec{{"structProd"}, "TST", "A4"},
      },
      AlgorithmSpec{
        [](ProcessingContext &ctx) {

          // Producer is sending at the end of the method. We sleep at the beginning
          std::cout << "Producer is sleeping one second" << std::endl;
          sleep(1);

          // Randomly pick an output channel
          std::string intProdName = "intProd_";
          intProdName += std::to_string((rand() % 2)+1);
          auto intProd = ctx.outputs().make<int>(OutputRef{intProdName}, 1);
          intProd[0] = rand() % 1000;
          std::cout << "Producing a random integer: " << intProd[0] << std::endl;

          auto intAryProd = ctx.outputs().make<int>(OutputRef{"intAryProd"}, 5);
          for (auto &i : intAryProd) {
            i = rand() % 1000;
          }
          std::cout << "Producing an array of random integers: " << catAry(intAryProd).str() << std::endl;

          std::vector<std::string> buf = {
            "Taci. Su le soglie / del bosco non odo / parole che dici / umane",
            "Nel mezzo del cammin di nostra vita / mi ritrovai per una selva oscura",
            "M'illumino / d'immenso."
          };
          size_t c = rand() % buf.size();
          auto stringProd = ctx.outputs().make<char>(OutputRef{"stringProd"}, buf[c].length()+1);
          std::copy(buf[c].begin(), buf[c].end()+1, stringProd.begin());
          std::cout << "Producing a string: " << buf[c] << std::endl;

          auto structProd = ctx.outputs().make<PairOfNumbers>(OutputRef{"structProd"}, 1);
          structProd[0] = { rand() % 1000, (double)(rand() % 10)+0.1234 };
          std::cout << "Producing a random struct: { " << structProd[0].a << ", "
                                                       << structProd[0].b << " }" << std::endl;

        }
      }

    },

    {
      "ConsumerOfInt1",
      {InputSpec{"intCons", "TST", "A1_1"}},
      Outputs{},
      AlgorithmSpec{
        [](ProcessingContext &ctx) {
          std::cout << "ConsumerOfInt1 labels: " << catLabels(ctx).str() << std::endl;
          auto intCons = ctx.inputs().get<int>("intCons");
          std::cout << "ConsumerOfInt1 received " << intCons << std::endl;
        }
      }
    },

    {
      "ConsumerOfInt2",
      {InputSpec{"intCons", "TST", "A1_2"}},
      Outputs{},
      AlgorithmSpec{
        [](ProcessingContext &ctx) {
          std::cout << "ConsumerOfInt2 labels: " << catLabels(ctx).str() << std::endl;
          auto intCons = ctx.inputs().get<int>("intCons");
          std::cout << "ConsumerOfInt2 received " << intCons << std::endl;
        }
      }
    },

    {
      "ConsumerOfIntAry",
      {InputSpec{"intAryCons", "TST", "A2"}},
      Outputs{},
      AlgorithmSpec{
        [](ProcessingContext &ctx) {
          std::cout << "ConsumerOfIntAry labels: " << catLabels(ctx).str() << std::endl;
          // Bad: fetch the payload directly (size has to be hardcoded, or parsed from header...)
          //auto intAryCons = *reinterpret_cast<const std::array<int, 5> *>(ctx.inputs().get("intAryCons").payload);
          // Good: make it return a GSL span (size embedded)
          auto intAryCons = DataRefUtils::as<int>(ctx.inputs().get("intAryCons"));
          std::cout << "ConsumerOfIntAry received " << catAry(intAryCons).str()
                    << " (type is: " << typeid(decltype(intAryCons)).name() << ")" << std::endl;

        }
      }
    },

    {
      "ConsumerOfString",
      {InputSpec{"stringCons", "TST", "A3"}},
      Outputs{},
      AlgorithmSpec{
        [](ProcessingContext &ctx) {
          std::cout << "ConsumerOfString labels: " << catLabels(ctx).str() << std::endl;
          // Bad: fetch the payload
          //auto stringCons = reinterpret_cast<const char *>(ctx.inputs().get("stringCons").payload);
          // Better: use a GSL span of chars, fetch address of the first one (spans have a contiguous representation, and I know the original sequence is null-terminated)
          auto stringCons = &DataRefUtils::as<char>(ctx.inputs().get("stringCons"))[0];
          std::cout << "ConsumerOfString received " << stringCons << std::endl;
        }
      }
    },

    {
      "ConsumerOfStruct",
      {InputSpec{"structCons", "TST", "A4"}},
      Outputs{},
      AlgorithmSpec{
        [](ProcessingContext &ctx) {
          std::cout << "ConsumerOfStruct labels: " << catLabels(ctx).str() << std::endl;
          auto structCons = ctx.inputs().get<PairOfNumbers>("structCons");
          std::cout << "ConsumerOfStruct received { " << structCons.a << ", "
                                                      << structCons.b << " }" << std::endl;
        }
      }
    },

  };
}

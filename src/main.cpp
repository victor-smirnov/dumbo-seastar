// Copyright 2016 Victor Smirnov
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <dumbo/v1/allocators/inmem/factory.hpp>
#include <dumbo/v1/tools/dumbo_iostreams.hpp>
#include <dumbo/v1/tools/aio_uuid.hpp>
#include <dumbo/v1/tools/aio_string.hpp>

#include <memoria/v1/memoria.hpp>
#include <memoria/v1/containers/set/set_factory.hpp>
#include <memoria/v1/core/tools/iobuffer/io_buffer.hpp>

#include <memoria/v1/core/tools/strings/string_codec.hpp>
#include <memoria/v1/core/tools/time.hpp>
#include <memoria/v1/core/tools/fixed_array.hpp>
#include <memoria/v1/core/tools/random.hpp>
#include <memoria/v1/core/tools/ticker.hpp>


#include "core/app-template.hh"
#include "core/sleep.hh"
#include "core/fstream.hh"
#include "core/shared_ptr.hh"
#include "core/thread.hh"

#include <algorithm>
#include <vector>
#include <type_traits>
#include <iostream>
#include <thread>

using namespace memoria::v1;
using namespace dumbo::v1;

namespace ss = seastar;

int main(int argc, char** argv)
{
	MEMORIA_INIT(DumboProfile<>);

	using Key = BigInt;
	DumboInit<Set<Key>>();

	try {
		app_template app;
		app.run(argc, argv, [&] {
			return ss::async([&] {

				auto alloc = SeastarInMemAllocator<>::create();
				auto snp = alloc->master()->branch();

				auto map = create<Set<Key>>(snp, UUID::parse("b1197537-12eb-4dc7-811b-ee0491720fbc"));

				for (int c = 0; c < 10000; c++)
				{
					map->insert_key(c);
				}

				snp->commit();
				snp->set_as_master();

				alloc->store("dumbo-data.dump");

				std::cout << "Store created\n";

				auto alloc1 = SeastarInMemAllocator<>::load("dumbo-data.dump");

				alloc1->dump("dumbo-dump");

				auto snp1 = alloc1->master();

				auto map1 = snp1->find<Set<Key>>(UUID::parse("b1197537-12eb-4dc7-811b-ee0491720fbc"));

				auto i1 = map1->begin();

				while (!i1->isEnd())
				{
					std::cout << i1->key() << "\n";
					i1->next();
				}
			}).handle_exception([](auto eptr){
				try {
					std::rethrow_exception(eptr);
				}
				catch(Exception &e) {
					std::cerr << "HANDLED: " << e.source() << ": " << e.message() << "\n";
				}

				return ::now();
			});
		});
	}
	catch(std::runtime_error &e) {
		std::cerr << "Couldn't start application: " << e.what() << "\n";
		return 1;
	}
}




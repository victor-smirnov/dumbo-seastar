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

#include <dumbo/v1/tools/recursive_mutex.hpp>

#include <algorithm>
#include <vector>
#include <type_traits>
#include <iostream>

using namespace memoria::v1;


int main(int argc, char** argv)
{
	MEMORIA_INIT(DefaultProfile<>);

	app_template app;
	app.run(argc, argv, [] {
			std::cout << "Sleeping... " << std::flush;
			using namespace std::chrono_literals;
			return sleep(1s).then([] {
				auto alloc = PersistentInMemAllocator<>::create();
				auto snp = alloc->master()->branch();

				snp->commit();

				// Store binary contents of allocator to the file.
				auto out = FileOutputStreamHandler::create("dumbo.dump");
				alloc->store(out.get());

				std::cout << "Done.\n";
			});
	});
}





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

#pragma once

#include <memoria/v1/core/tools/uuid.hpp>

namespace dumbo {
namespace v1 {

namespace mem = memoria::v1;

template <typename T> struct AIOReaderAdapter;
template <typename T> struct AIOWriterAdapter;

template <>
struct AIOReaderAdapter<mem::UUID> {
	template <typename ISA>
	static auto process(ISA&& isa)
	{
		return do_with(mem::UUID(), [isa](auto& uuid){
//			return read<UBigInt>(isa).then([&, isa](auto hi){
//				return read<UBigInt>(isa).then([&, isa](auto lo){
//
//					cout << "Here \n";
//
//					uuid.hi() = hi;
//					uuid.lo() = lo;
//
//					return make_ready_future<mem::UUID>(uuid);
//				});
//			});

			return ready(isa >> uuid.hi() >> uuid.lo()).then([&](){
				return make_ready_future<mem::UUID>(uuid);
			});
		});
	}
};

template <>
struct AIOWriterAdapter<mem::UUID> {
	using T = mem::UUID;

	template <typename OSA>
	static auto process(OSA&& osa, const T& uuid)
	{
		return ready(osa << uuid.hi() << uuid.lo());
	}
};


}
}

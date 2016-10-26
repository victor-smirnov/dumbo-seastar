
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


#include "core/sstring.hh"
#include <string>

namespace dumbo {
namespace v1 {

namespace mem = memoria::v1;

template <typename T> struct AIOReaderAdapter;
template <typename T> struct AIOWriterAdapter;

template <>
struct AIOReaderAdapter<std::string> {
	template <typename ISA>
	static auto process(ISA&& isa)
	{
		return read<BigInt>(isa).then([=](auto len){
			return isa->stream().read_exactly(len).then([=](const auto& buf){
				return make_ready_future<std::string>(std::string(buf.get(), len));
			});
		});
	}
};

template <>
struct AIOWriterAdapter<std::string> {
	using T = std::string;

	template <typename OSA>
	static auto process(OSA&& osa, T str)
	{
		return do_with(std::move(str), [=](const auto& vv){
			return ready(osa << (BigInt)vv.length() << CharData(vv));
		});
	}
};


template <>
struct AIOWriterAdapter<const char*> {
	using T = const char*;

	template <typename OSA>
	static auto process(OSA&& osa, T str)
	{
		BigInt len = strlen(str);
		return ready(osa << std::move(len) << CharData(str, len));
	}
};



template <>
struct AIOReaderAdapter<::sstring> {
	template <typename ISA>
	static auto process(ISA&& isa)
	{
		return read<BigInt>(isa).then([=](auto len){
			return isa->stream().read_exactly(len).then([=](const auto& buf){
				return make_ready_future<std::string>(::sstring(buf.get(), len));
			});
		});
	}
};

template <>
struct AIOWriterAdapter<::sstring> {
	using T = ::sstring;

	template <typename OSA>
	static auto process(OSA&& osa, const T& str)
	{
		return ready(osa << (UInt)str.length() << CharData(str));
	}
};


}
}


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

#include "core/iostream.hh"
#include "core/shared_ptr.hh"
#include "core/fstream.hh"
#include "core/sstring.hh"

#include <memoria/v1/core/types/type2type.hpp>


#include <memory>
#include <iostream>

namespace dumbo {
namespace v1 {

using namespace memoria::v1;
using namespace seastar;


struct IOByteOrderConverter {
	virtual ~IOByteOrderConverter() {}

	virtual Short convert(Short value) 		= 0;
	virtual UShort convert(UShort value) 	= 0;
	virtual Int convert(Int value) 			= 0;
	virtual UInt convert(UInt value) 		= 0;
	virtual BigInt convert(BigInt value) 	= 0;
	virtual UBigInt convert(UBigInt value) 	= 0;

	virtual float convert(float value) 		= 0;
	virtual double convert(double value) 	= 0;
};

struct NoOpIOByteOrderConverter: IOByteOrderConverter {

	virtual ~NoOpIOByteOrderConverter() {}

	virtual Short convert(Short value) 		{return value;}
	virtual UShort convert(UShort value) 	{return value;}
	virtual Int convert(Int value) 			{return value;}
	virtual UInt convert(UInt value) 		{return value;}
	virtual BigInt convert(BigInt value) 	{return value;}
	virtual UBigInt convert(UBigInt value) 	{return value;}

	virtual float convert(float value) 		{return value;}
	virtual double convert(double value) 	{return value;}
};






template <typename T>
struct AIOReaderAdapter {
	template <typename ISA>
	static auto process(ISA&& isa) {
		return isa.is_.read_exactly(sizeof(T)).then([&](const auto& buf){
			T value = *T2T<const T*>(buf.get());
			return make_ready_future<T>(isa.pconverter_->convert(value));
		});
	}
};

template <>
struct AIOReaderAdapter<Byte> {
	using T = Byte;

	template <typename ISA>
	static auto process(ISA&& isa) {
		return isa.is_.read_exactly(sizeof(T)).then([&](const auto& buf){
			T value = *T2T<const T*>(buf.get());
			return make_ready_future<T>(value);
		});
	}
};


template <>
struct AIOReaderAdapter<UByte> {
	using T = UByte;

	template <typename ISA>
	static auto process(ISA&& isa) {
		return isa.is_.read_exactly(sizeof(T)).then([&](const auto& buf){
			T value = *T2T<const T*>(buf.get());
			return make_ready_future<T>(value);
		});
	}
};

template <>
struct AIOReaderAdapter<bool> {
	using T = UByte;

	template <typename ISA>
	static auto process(ISA&& isa) {
		return isa.is_.read_exactly(sizeof(T)).then([&](const auto& buf){
			T value = *T2T<const T*>(buf.get());
			return make_ready_future<T>(value);
		});
	}
};





class TypedAsyncInputStream {
	input_stream<char> is_;

	std::unique_ptr<IOByteOrderConverter> converter_;

	IOByteOrderConverter* pconverter_;

public:

	template <typename T> friend class AIOReaderAdapter;

	TypedAsyncInputStream(input_stream<char> istream): is_(std::move(istream)),
		converter_(std::make_unique<NoOpIOByteOrderConverter>()),
		pconverter_(converter_.get())
	{}

	TypedAsyncInputStream(input_stream<char> istream, std::unique_ptr<IOByteOrderConverter> converter):
		is_(std::move(istream)),
		converter_(std::move(converter)),
		pconverter_(converter_.get())
	{}

	TypedAsyncInputStream(const TypedAsyncInputStream&) = delete;
	TypedAsyncInputStream(TypedAsyncInputStream&& other):
		is_(std::move(other.is_)),
		converter_(std::move(other.converter_)),
		pconverter_(converter_.get())
	{}

	~TypedAsyncInputStream() {}

	input_stream<char>& stream() {return is_;}
	const input_stream<char>& stream() const {return is_;}

	TypedAsyncInputStream& operator=(const TypedAsyncInputStream&) = delete;
	TypedAsyncInputStream& operator=(TypedAsyncInputStream&& other)
	{
		is_ = std::move(other.is_);
		converter_ = std::move(other.converter_);

		pconverter_ = converter_.get();

		return *this;
	}

	template <typename T>
	future<T> read() {
		return AIOReaderAdapter<T>::process(*this);
	}
};


template <typename T>
future<T> read(TypedAsyncInputStream& adaptor) { return adaptor.template read<T>();}

template <typename T>
future<T> read(TypedAsyncInputStream* adaptor) { return adaptor->template read<T>();}

template <typename T>
future<T> read(TypedAsyncInputStream&& adaptor) { return adaptor.template read<T>();}

template <typename T>
future<T> read(const ::shared_ptr<TypedAsyncInputStream>& adaptor) { return adaptor->template read<T>();}

template <typename T>
future<T> read(const ::lw_shared_ptr<TypedAsyncInputStream>& adaptor) { return adaptor->template read<T>();}

static inline lw_shared_ptr<TypedAsyncInputStream> read(file f)
{
	return ::make_lw_shared<TypedAsyncInputStream>(::make_file_input_stream(std::move(f)));
}



template <template <typename> class Ptr, typename T>
future<Ptr<TypedAsyncInputStream>> operator>>(Ptr<TypedAsyncInputStream> stream, T& target)
{
	return read<T>(stream).then([stream = std::move(stream), &target](auto value){
		target = value;
		return make_ready_future<Ptr<TypedAsyncInputStream>>(std::move(stream));
	});
}

template <template <typename> class Ptr, typename T>
future<Ptr<TypedAsyncInputStream>> operator>>(future<Ptr<TypedAsyncInputStream>> ff, T& target)
{
	return ff.then([&](auto stream){
		return read<T>(stream).then([stream = std::move(stream), &target](auto value){
			target = value;
			return make_ready_future<Ptr<TypedAsyncInputStream>>(std::move(stream));
		});
	});
}


class CharData {
	const char* data_;
	size_t length_;
public:
	CharData(): data_(nullptr), length_(0) {}
	CharData(const char* data, size_t length): data_(data), length_(length) {}

	CharData(const std::string& str): data_(str.c_str()), length_(str.length()) {}

	CharData(const ::sstring& str): data_(str.c_str()), length_(str.length()) {}

	const char* data() const {return data_;}
	size_t length() const {return length_;}
};

template <typename T> struct AIOWriterAdapter;

template <typename T>
struct AIOWriterAdapterBase {
	template <typename OSA, typename TT>
	static auto process(OSA&& osa, TT&& value)
	{
		return do_with(osa->converter()->convert(value), [&](auto cvalue) {
			return osa->write(&cvalue, 0, sizeof(T));
		});
	}
};

template <> struct AIOWriterAdapter<Short>:   AIOWriterAdapterBase<Short> {};
template <> struct AIOWriterAdapter<UShort>:  AIOWriterAdapterBase<UShort> {};
template <> struct AIOWriterAdapter<Int>:     AIOWriterAdapterBase<Int> {};
template <> struct AIOWriterAdapter<UInt>:    AIOWriterAdapterBase<UInt> {};
template <> struct AIOWriterAdapter<BigInt>:  AIOWriterAdapterBase<BigInt> {};
template <> struct AIOWriterAdapter<UBigInt>: AIOWriterAdapterBase<UBigInt> {};
template <> struct AIOWriterAdapter<float>:   AIOWriterAdapterBase<float> {};
template <> struct AIOWriterAdapter<double>:  AIOWriterAdapterBase<double> {};


template <>
struct AIOWriterAdapter<Byte> {
	using T = Byte;

	template <typename OSA>
	static auto process(OSA&& osa, T value)
	{
		using char_t = typename decltype(osa->os_)::char_type;
		return do_with(std::move(value), [&](auto cvalue) {
			return osa->os_.write(T2T<const char_t*>(&cvalue), sizeof(T));
		});
	}
};

template <>
struct AIOWriterAdapter<UByte> {
	using T = UByte;

	template <typename OSA>
	static auto process(OSA&& osa, T value)
	{
		using char_t = typename decltype(osa->os_)::char_type;
		return do_with(std::move(value), [&](auto cvalue) {
			return osa->os_.write(T2T<const char_t*>(&cvalue), sizeof(T));
		});
	}
};

template <>
struct AIOWriterAdapter<bool>: AIOWriterAdapter<UByte> {};


template <>
struct AIOWriterAdapter<CharData> {
	using T = CharData;

	template <typename OSA>
	static auto process(OSA&& osa, T value)
	{
		using char_t = typename decltype(osa->os_)::char_type;
		return osa->os_.write(value.data(), value.length());
	}
};




class TypedAsyncOutputStream {
	output_stream<char> os_;

	std::unique_ptr<IOByteOrderConverter> converter_;

	IOByteOrderConverter* pconverter_;

public:

	using char_type = typename output_stream<char>::char_type;

	template <typename T> friend class AIOWriterAdapter;

	TypedAsyncOutputStream(output_stream<char> istream): os_(std::move(istream)),
		converter_(std::make_unique<NoOpIOByteOrderConverter>()),
		pconverter_(converter_.get())
	{}

	TypedAsyncOutputStream(output_stream<char> istream, std::unique_ptr<IOByteOrderConverter> converter):
		os_(std::move(istream)),
		converter_(std::move(converter)),
		pconverter_(converter_.get())
	{}

	TypedAsyncOutputStream(const TypedAsyncOutputStream&) = delete;
	TypedAsyncOutputStream(TypedAsyncOutputStream&& other):
		os_(std::move(other.os_)),
		converter_(std::move(other.converter_)),
		pconverter_(converter_.get())
	{}

	~TypedAsyncOutputStream() {}

	output_stream<char>& stream() {return os_;}
	const output_stream<char>& stream() const {return os_;}

	IOByteOrderConverter* converter() {return pconverter_;}
	const IOByteOrderConverter* converter() const {return pconverter_;}

	TypedAsyncOutputStream& operator=(const TypedAsyncOutputStream&) = delete;
	TypedAsyncOutputStream& operator=(TypedAsyncOutputStream&& other)
	{
		os_ = std::move(other.os_);
		converter_ = std::move(other.converter_);

		pconverter_ = converter_.get();

		return *this;
	}

	template <typename T>
	future<> write(T&& value)
	{
		return AIOWriterAdapter<T>::process(*this, value);
	}

	future<> write(const void* buffer, size_t offset, size_t length)
	{
		return os_.write(T2T<const char*>(buffer) + offset, length);
	}

	future<> flush() {
		return os_.flush();
	}

	future<> close() {
		return os_.close();
	}
};

static inline lw_shared_ptr<TypedAsyncOutputStream> write(file f)
{
	return ::make_lw_shared<TypedAsyncOutputStream>(::make_file_output_stream(std::move(f)));
}

static inline lw_shared_ptr<TypedAsyncOutputStream> write(file&& f)
{
	return ::make_lw_shared<TypedAsyncOutputStream>(::make_file_output_stream(std::move(f)));
}





template <template <typename> class Ptr, typename T>
future<Ptr<TypedAsyncOutputStream>> operator<<(Ptr<TypedAsyncOutputStream> adapter, T&& source)
{
	return AIOWriterAdapter<std::decay_t<T>>::process(adapter, std::forward<T>(source)).then([adapter](){
		return make_ready_future<Ptr<TypedAsyncOutputStream>>(std::move(adapter));
	});
}


template <template <typename> class Ptr, typename T>
future<Ptr<TypedAsyncOutputStream>> operator<<(future<Ptr<TypedAsyncOutputStream>> ff, T&& source)
{
	return ff.then([source = std::forward<T>(source)](auto adapter) {
		return AIOWriterAdapter<std::decay_t<T>>::process(adapter, source).then([adapter]() {
			return make_ready_future<Ptr<TypedAsyncOutputStream>>(std::move(adapter));
		});
	});
}






template <typename S>
future<> ready(future<S> ff)
{
	return ff.then([](const auto&){return ::now();});
}

}
}

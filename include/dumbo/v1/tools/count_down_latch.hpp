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

#include <core/rwlock.hh>
#include <core/condition-variable.hh>

namespace dumbo {
namespace v1 {

template <typename T>
class SeastarCountDownLatch {
	T value_;

	condition_variable cv_;

public:
	SeastarCountDownLatch(): value_() {}
	SeastarCountDownLatch(T init): value_(init) {}

	void inc() {
		value_++;
	}

	void dec()
	{
		if (--value_ == 0)
		{
			cv_.signal();
		}
	}

	T get() const {return value_;}

	future<> wait()
	{
		if (value_) {
			return cv_.wait();
		}
		else {
			return ::now();
		}
	}

	future<> wait(typename semaphore::duration timeout)
	{
		if (value_) {
			return cv_.wait(timeout);
		}
		else {
			return ::now();
		}
	}
};


}}

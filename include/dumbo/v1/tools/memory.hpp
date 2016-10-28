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

#include <memoria/v1/core/types/type2type.hpp>

#include "core/reactor.hh"



namespace dumbo {
namespace v1 {

template <typename T>
future<T*> allocate_on(int cpu_id, size_t size = 1)
{
	return smp::submit_to(cpu_id, [=]{
		return make_ready_future<T*>(T2T<T*>(malloc(size * sizeof(T))));
	});
}


static inline future<> free_on(int cpu_id, void* ptr)
{
	if (ptr)
	{
		return smp::submit_to(cpu_id, [=]{
			free(ptr);
			return now();
		});
	}
	else {
		return ::now();
	}
}


template <typename T, typename... Args>
future<T*> new_on(int cpu_id, Args&&... args)
{
	return smp::submit_to(cpu_id, [&, cpu_id]{
		return make_ready_future<T*>(new T(std::forward<Args>(args)...));
	});
}

template <typename T>
future<> delete_on(int cpu_id, T* ptr)
{
	return smp::submit_to(cpu_id, [=]{
		delete ptr;
		return ::now();
	});
}

}
}

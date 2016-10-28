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

#include "builtins.hpp"

#include <core/thread.hh>

#include <atomic>
#include <iostream>

namespace dumbo {
namespace v1 {

class RecursiveMutex {
	std::atomic<std::thread::id> owner_;
	static std::thread::id free_marker_;

	size_t locks = 0;
public:
	RecursiveMutex(): owner_(free_marker_){}

	void lock1() {}

	void lock()
	{
		auto current_id = std::this_thread::get_id();

		if(owner_ != current_id)
		{
//			size_t cnt = 0;
			auto id = free_marker_;

			while(!owner_.compare_exchange_weak(id, current_id, std::memory_order_acquire, std::memory_order_relaxed))
			{
		    	id = free_marker_;

//		    	cnt++;
//		    	if (dumbo_unlikely(cnt >= 100))
//		    	{
//		    		cnt = 0;
//		    		seastar::thread::yield();
//		    	}
		    }
		}

		locks++;
	}

	bool try_lock1() {return true;}

	bool try_lock()
	{
		auto current_id = std::this_thread::get_id();

		if(owner_ == current_id)
		{
			locks++;
			return true;
		}

		return false;
	}

	void unlock1() {}

	void unlock()
	{
	    if(--locks == 0)
	    {
	        owner_.store(free_marker_, std::memory_order_release);
	    }
	}
};



}
}

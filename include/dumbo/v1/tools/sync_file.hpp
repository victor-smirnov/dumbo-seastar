
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

#include <memoria/v1/core/types/types.hpp>
#include <memoria/v1/core/tools/config.hpp>
#include <memoria/v1/core/tools/strings/string.hpp>


#include "core/seastar.hh"

namespace dumbo {
namespace v1 {

namespace mem = memoria::v1;

using SString = sstring;
using SStringRef = const sstring&;


bool isEmpty(SStringRef str) {
    if (str.size() == 0) {
    	return true;
    }
    else {
    	for (auto c = 0; c < str.size(); c++) {
    		auto ch = str[c];
    		if (ch != '\r' || ch != '\n' || ch != 't' || ch != ' ') {
    			return false;
    		}
    	}

    	return true;
    }
}


class SyncFile {
    SString path_;

public:
    using SyncFileListType = std::vector<std::unique_ptr<SyncFile>>;

    SyncFile(SStringRef path): path_(path) {}

    SyncFile(const SyncFile& file): path_(file.path_) {}

    SyncFile(SyncFile&& file): path_(file.path_) {}

    ~SyncFile() noexcept {}

    SyncFile operator=(SyncFile&& other)
    {
    	path_ = std::move(other.path_);
    	return *this;
    }

    String getName() const {
    	String::size_type idx = path_.find_last_of('/');
    	if (idx == String::npos)
    	{
    		return path_;
    	}
    	else if (idx == path_.length() - 1){
    		return "";
    	}
    	else {
    		return path_.substr(idx + 1, path_.length() - idx - 1);
    	}
    }

    SStringRef getPath() const {
    	return path_;
    }

    bool isExists() const {
    	return file_exists(path_).get0();
    }


    bool isDirectory() const {
    	return is_directory(path_).get0();
    }

//    // FIXME: may block
//    String getAbsolutePath() const {
//        if (path_[0] == '/')
//        {
//            return path_;
//        }
//        else {
//            char buf[8192];
//            if (getcwd(buf, sizeof(buf)))
//            {
//                return String(buf)+"/"+path_;
//            }
//            else {
//                throw FileException(MEMORIA_SOURCE, SBuf()<<"Can't get absolute path: "<<strerror(errno)<<" "<<path_);
//            }
//        }
//    }

    void syncDirectory() const {
    	sync_directory(path_).get();
    }

    bool mkDir() const {
    	touch_directory(path_).get0();
    	return true;
    }

    bool mkDirs() const {
    	recursive_touch_directory(path_).get0();
    	return true;
    }

    bool deleteFile() const
    {
        if (isDirectory())
        {
        	remove_directory(path_).get();
            return true;
        }
        else {
        	remove_file(path_).get();
            return true;
        }
    }

    bool delTree() const {
    	return rm(*this);
    }

    void rename(SString new_name)
    {
    	rename_file(path_, new_name).get();
    }

    uint64_t getSize() const {
    	return file_size(path_).get0();
    }

    static std::unique_ptr<SyncFileListType> readDir(const SyncFile& file)
    {
        if (file.isDirectory())
        {
        	std::unique_ptr<SyncFileListType> list = std::make_unique<SyncFileListType>();

        	auto ff = open_directory(file.getPath()).get0();

        	ff.list_directory([&](auto entry){
        		list->push_back(std::make_unique<SyncFile>(file.getPath() + "/" + entry.name));
        		return ::now();
        	}).done().get();

            return list;
        }
        else
        {
            throw FileException(MEMORIA_SOURCE, SBuf()<<"File is not a directory: "<<file.getPath());
        }
    }

private:
    static bool rm(const SyncFile &file)
    {
        if (file.isDirectory())
        {
            auto list = SyncFile::readDir(file);

            bool result = true;
            for (auto c = 0; c < list->size(); c++)
            {
            	auto& entry = list->operator[](c);
                result = rm(*entry) && result;
            }

            file.syncDirectory();

            return result && file.deleteFile();
        }
        else {
            return file.deleteFile();
        }
    }
};



}
}

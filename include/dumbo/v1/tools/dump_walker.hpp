
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

#include <dumbo/v1/tools/sync_file.hpp>
#include <dumbo/v1/tools/dumbo_iostreams.hpp>

#include <memoria/v1/metadata/container.hpp>



namespace dumbo {
namespace v1 {

using namespace memoria::v1;

template <typename PageType>
class DumboFSDumpContainerWalker: public ContainerWalker {

    using Page = PageType;
    using ID   = typename Page::ID;

    ContainerMetadataRepository* metadata_;
    std::stack<SyncFile> path_;

public:
    DumboFSDumpContainerWalker(ContainerMetadataRepository* metadata, SStringRef root):
        metadata_(metadata)
    {
        SyncFile root_path(root);

        if (!root_path.isExists())
        {
            root_path.mkDirs();
        }
        else {
            root_path.delTree();
            root_path.mkDirs();
        }

        path_.push(root_path);
    }

    virtual void beginSnapshotSet(const char* descr, size_t number)
    {
        pushFolder(descr);
    }

    virtual void endSnapshotSet()
    {
        path_.pop();
    }

    virtual void beginAllocator(const char* type, const char* desc)
    {
        pushFolder(type);
    }

    virtual void endAllocator()
    {
        path_.pop();
    }

    virtual void beginSnapshot(const char* descr)
    {
        pushFolder(descr);
    }

    virtual void endSnapshot()
    {
        path_.pop();
    }

    virtual void beginCompositeCtr(const char* descr, const UUID& name)
    {
        stringstream str;

        str << shorten(descr) <<": " << name;

        pushFolder(str.str().c_str());

        dumpDescription("ctr_name", String(descr));
    }

    virtual void endCompositeCtr() {
        path_.pop();
    }

    virtual void beginCtr(const char* descr, const UUID& name, const UUID& root)
    {
        stringstream str;

        str<<shorten(descr)<<": "<<name;

        pushFolder(str.str().c_str());

        dumpDescription("ctr_name", SString(descr));
    }

    virtual void endCtr() {
        path_.pop();
    }

    virtual void rootLeaf(Int idx, const void* page_data)
    {
        const Page* page = T2T<Page*>(page_data);

        SString file_name = path_.top().getPath() + Platform::getFilePathSeparator() + "root_leaf.txt";

        dumpPage(file_name, page);
    }

    virtual void leaf(Int idx, const void* page_data)
    {
        const Page* page = T2T<Page*>(page_data);

        String description = getNodeName("Leaf", idx, page->id());

        SString file_name = path_.top().getPath() + Platform::getFilePathSeparator() + description + ".txt";

        dumpPage(file_name, page);
    }

    virtual void beginRoot(Int idx, const void* page_data)
    {
        beginNonLeaf("Root", idx, page_data);
    }

    virtual void endRoot()
    {
        path_.pop();
    }

    virtual void beginNode(Int idx, const void* page_data)
    {
        beginNonLeaf("Node", idx, page_data);
    }

    virtual void endNode()
    {
        path_.pop();
    }

    virtual void singleNode(const char* description, const void* page_data)
    {
        const Page* page = T2T<Page*>(page_data);

        String file_name = path_.top().getPath() + Platform::getFilePathSeparator() + description + ".txt";

        dumpPage(file_name, page);
    }


    virtual void beginSection(const char* name)
    {
        pushFolder(name);
    }

    virtual void endSection() {
        path_.pop();
    }

    virtual void content(const char* name, const char* content)
    {
        dumpDescription(name, content);
    }

private:

    void beginNonLeaf(const char* type, Int idx, const void* page_data)
    {
        const Page* page = T2T<Page*>(page_data);

        SString folder_name = getNodeName(type, idx, page->id());
        pushFolder(folder_name.c_str());

        SString file_name = path_.top().getPath() + Platform::getFilePathSeparator() + "0_page.txt";

        dumpPage(file_name, page);
    }


    void dumpPage(SStringRef file, const Page* page)
    {
        std::stringstream pagetxt; //file.c_str()

        auto meta = metadata_->getPageMetadata(page->ctr_type_hash(), page->page_type_hash());

        dumpPageData(meta.get(), page, pagetxt);

        store(file.c_str(), pagetxt.str());
    }

    void dumpDescription(StringRef type, StringRef content)
    {
        String file_name = path_.top().getPath() + Platform::getFilePathSeparator() + type + ".txt";

        std::stringstream file;

        file << content;

        store(file_name.c_str(), file.str());
    }

    void pushFolder(const char* descr)
    {
        SString name = path_.top().getPath() + Platform::getFilePathSeparator() + String(descr);
        SyncFile file(name);
        MEMORIA_V1_ASSERT_TRUE(file.mkDir());
        path_.push(file);
    }

    SString getNodeName(const char* name, Int index, const ID& id)
    {
        std::stringstream str;

        str<<name<<"-";

        char prev = str.fill();

        str.fill('0');
        str.width(4);

        str<<index;

        str.fill(prev);

        str<<"___"<<id;

        return str.str();
    }

private:

    void store(const char* file, const String& data)
    {
    	auto stream = ::make_file_output_stream(open_file_dma(file, open_flags::create | open_flags::truncate | open_flags::wo).get0());

    	stream.write(data.c_str(), data.size()).get();

    	stream.close().get();
    }

    SString shorten(const char* txt)
    {
        String text = txt;

        auto start = text.find_first_of("<");

        if (start != String::npos)
        {
            text.erase(start);
        }

        return text;
    }
};

}}

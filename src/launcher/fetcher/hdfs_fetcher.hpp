/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#ifndef __HDFS_FETCHER_HPP__
#define __HDFS_FETCHER_HPP__

#include <stout/os.hpp>

#include "hdfs/hdfs.hpp"
#include "launcher/fetcher/fetcher.hpp"

using namespace mesos;

using std::string;

// a fetcher that uses hdfs/hadoop to fetch files
class HadoopFetcher: public Fetcher {
protected:
  HDFS hdfs;

  virtual Try<bool> available() {
    return hdfs.available();
  }

  virtual Try<Nothing> copyToLocal(
      const std::string& from,
      const std::string& to)
  {
    return hdfs.copyToLocal(from, to);
  }

public:
  Try<string> fetch(
    const string& uri,
    const string& directory)
  {
    if(available().isError()) {
      LOG(INFO) << "Hadoop/HDFS not available, skipping fetch with HDFS";
      return Error("HDFS unavailable");
    }

    LOG(INFO) << "Fetching URI '" << uri << "' using HDFS";

    Try<string> base = os::basename(uri);
    if (base.isError()) {
      LOG(ERROR) << "Invalid basename for URI: " << base.error();
      return Error("Invalid basename for URI");
    }

    string path = path::join(directory, base.get());

    LOG(INFO) << "Downloading resource from '" << uri
    << "' to '" << path << "'";

    Try<Nothing> result = copyToLocal(uri, path);
    if(result.isError()) {
      LOG(ERROR) << "HDFS copyToLocal failed: " << result.error();
      return Error(result.error());
    }

    return path;
  }
};

#endif

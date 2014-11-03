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
#ifndef __LOCAL_FETCHER_HPP__
#define __LOCAL_FETCHER_HPP__

#include <stout/os.hpp>

#include "launcher/fetcher/fetcher.hpp"

using namespace mesos;

using std::string;

class LocalFetcher: public Fetcher {

private:
  const char FILE_URI_PREFIX[] = "file://";
  const char FILE_URI_LOCALHOST[] = "file://localhost";

protected:
  // method to make testing easier
  virtual Try<int> execute(const string& command)
  {
    int status = os::system(command);
    if (status != 0) {
        LOG(ERROR) << "Failed to copy '" << local
        << "' : Exit status " << status;
        return Error("Local copy failed");
    }
    return status;
  }

public:
  Try<string> fetch(
    const string& uri,
    const string& directory)
  {
    string local = uri;
    bool fileUri = false;
    if (strings::startsWith(local, string(FILE_URI_LOCALHOST))) {
        local = local.substr(sizeof(FILE_URI_LOCALHOST) - 1);
        fileUri = true;
    } else if (strings::startsWith(local, string(FILE_URI_PREFIX))) {
        local = local.substr(sizeof(FILE_URI_PREFIX) - 1);
        fileUri = true;
    }

    if(fileUri && !strings::startsWith(local, "/")) {
        return Error("File URI only supports absolute paths");
    }

    if (local.find_first_of("/") != 0) {
        // We got a non-Hadoop and non-absolute path.
        if (os::hasenv("MESOS_FRAMEWORKS_HOME")) {
            local = path::join(os::getenv("MESOS_FRAMEWORKS_HOME"), local);
            LOG(INFO) << "Prepended environment variable "
            << "MESOS_FRAMEWORKS_HOME to relative path, "
            << "making it: '" << local << "'";
        } else {
            LOG(ERROR) << "A relative path was passed for the resource but the "
            << "environment variable MESOS_FRAMEWORKS_HOME is not set. "
            << "Please either specify this config option "
            << "or avoid using a relative path";
            return Error("Could not resolve relative URI");
        }
    }

    Try<string> base = os::basename(local);
    if (base.isError()) {
        LOG(ERROR) << base.error();
        return Error("Fetch of URI failed");
    }

    // Copy the resource to the directory.
    string path = path::join(directory, base.get());
    std::ostringstream command;
    command << "cp '" << local << "' '" << path << "'";
    LOG(INFO) << "Copying resource from '" << local
    << "' to '" << directory << "'";

    Try<int> status = execute(command.str());
    if(status.isError()) {
      return status;
    }

    // copy successful
    return path;
  }
};

#endif // __LOCAL_FETCHER_HPP__
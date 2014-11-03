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
#ifndef __CURL_FETCHER_HPP__
#define __CURL_FETCHER_HPP__

#include <stout/net.hpp>
#include <stout/os.hpp>

#include "launcher/fetcher/fetcher.hpp"

using namespace mesos;

using std::string;

// fetcher implementation that uses libcurl to fetch compatible files
class CurlFetcher: public Fetcher {

protected:
  // method to make testing easier
  virtual Try<int> download(
    const string& src,
    const string& dest)
  {
    return net::download(uri, dest);
  }

public:

  Try<string> fetch(
    const string& uri,
    const string& directory)
  {
    LOG(INFO) << "Fetching URI '" << uri << "' with os::net";

    string path = uri.substr(uri.find("://") + 3);
    if (path.find("/") == string::npos ||
        path.size() <= path.find("/") + 1) {
      LOG(ERROR) << "Malformed URL (missing path)";
      return Error("Malformed URI");
    }

    path =  path::join(directory, path.substr(path.find_last_of("/") + 1));
    LOG(INFO) << "Downloading '" << uri << "' to '" << path << "'";
    Try<int> code = download(uri, path);
    if (code.isError()) {
      LOG(ERROR) << "Error downloading resource: " << code.error().c_str();
      return Error("Fetch of URI failed (" + code.error() + ")");
    } else if (code.get() != 200) {
      LOG(ERROR) << "Error downloading resource, received HTTP/FTP return code "
      << code.get();
      return Error("HTTP/FTP error (" + stringify(code.get()) + ")");
    }

    return path;
  }

  Try<string> canhandleURI(const string& uri) {
    Try<string> result = Fetcher::canhandleURI(uri);
    if(result.isError()) {
      return result;
    }

    // in addition to the usual validation, this fetcher cannot
    // handle anything except http://, https://, ftp:// and ftps://
    if (!(strings::startsWith(uri, "http://") ||
        strings::startsWith(uri, "https://") ||
        strings::startsWith(uri, "ftp://") ||
        strings::startsWith(uri, "ftps://"))) {
        return Error("Unsupported scheme for uri handler");
    }

    return Nothing();
  }
};

#endif // __CURL_FETCHER_HPP__

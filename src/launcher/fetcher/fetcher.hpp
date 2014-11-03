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
#ifndef __FETCHER_HPP__
#define __FETCHER_HPP__

#include <string>
#include <mesos/mesos.hpp>
#include <stout/strings.hpp>

using namespace mesos;

using std::string;


// generic interface for fetchers
class Fetcher {
public:
  // Fetcher method that will fetch the given uri into the given directory
  // with the same name
  virtual Try<string> fetch(
    const string& uri,
    const string& directory) = 0;

  // This method validates the uri to make sure, it can be handled by the
  // fetcher
  virtual Try<string> canhandleURI(const string& uri) {
    // Some checks to make sure using the URI value in shell commands
    // is safe. TODO(benh): These should be pushed into the scheduler
    // driver and reported to the user.
    if (uri.find_first_of('\\') != string::npos ||
        uri.find_first_of('\'') != string::npos ||
        uri.find_first_of('\0') != string::npos) {
        LOG(ERROR) << "URI contains illegal characters, refusing to fetch";
        return Error("Illegal characters in URI");
    }
    return "ok";
  }
};

#endif __FETCHER_HPP__
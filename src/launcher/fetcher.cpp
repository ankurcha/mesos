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

#include <string>
#include <vector>

#include <mesos/mesos.hpp>

#include <stout/option.hpp>
#include <stout/os.hpp>
#include <stout/strings.hpp>

#include "launcher/fetcher/fetcher.hpp"
#include "launcher/fetcher/hdfs_fetcher.hpp"
#include "launcher/fetcher/local_fetcher.hpp"
#include "launcher/fetcher/curl_fetcher.hpp"

using namespace mesos;

using std::string;
using std::vector;



// Try to extract filename into directory. If filename is recognized as an
// archive it will be extracted and true returned; if not recognized then false
// will be returned. An Error is returned if the extraction command fails.
Try<bool> extract(const string& filename, const string& directory)
{
  string command;
  // Extract any .tgz, tar.gz, tar.bz2 or zip files.
  if (strings::endsWith(filename, ".tgz") ||
      strings::endsWith(filename, ".tar.gz") ||
      strings::endsWith(filename, ".tbz2") ||
      strings::endsWith(filename, ".tar.bz2") ||
      strings::endsWith(filename, ".txz") ||
      strings::endsWith(filename, ".tar.xz")) {
    command = "tar -C '" + directory + "' -xf";
  } else if (strings::endsWith(filename, ".zip")) {
    command = "unzip -d '" + directory + "'";
  } else {
    return false;
  }

  command += " '" + filename + "'";
  int status = os::system(command);
  if (status != 0) {
    return Error("Failed to extract: command " + command +
                 " exited with status: " + stringify(status));
  }

  LOG(INFO) << "Extracted resource '" << filename
            << "' into '" << directory << "'";

  return true;
}

// Fetch URI into directory.
Try<string> fetch(
    const vector<Fetcher> fetchers,
    const string& uri,
    const string& directory)
{
    LOG(INFO) << "Fetching URI '" << uri << "'";

    // Try each fetcher
    for (Fetcher &fetcher : fetchers) {
      // check if fetcher can handle this uri
      if(fetcher->canHandleURI(uri).isError()) {
        continue;
      }

      // fetch using this handler
      result = fetcher->fetch(uri, directory);
      if(result.isSome()) {
        return result;
      }
    }

    return Error("Unable to find compatible fetcher for uri: " + uri);
}

int main(int argc, char* argv[])
{
  GOOGLE_PROTOBUF_VERIFY_VERSION;

  // create a vector of fetchers that will be used
  const vector<Fetcher> fetchers({
    HadoopFetcher(),
    CurlFetcher(),
    LocalFetcher()});

  CommandInfo commandInfo;
  // Construct URIs from the encoded environment string.
  const std::string& uris = os::getenv("MESOS_EXECUTOR_URIS");
  foreach (const std::string& token, strings::tokenize(uris, " ")) {
    // Delimiter between URI, execute permission and extract options
    // Expected format: {URI}+[01][XN]
    //  {URI} - The actual URI for the asset to fetch.
    //  [01]  - 1 if the execute permission should be set else 0.
    //  [XN]  - X if we should extract the URI (if it's compressed) else N.
    size_t pos = token.rfind("+");
    CHECK(pos != std::string::npos)
      << "Invalid executor uri token in env " << token;

    CommandInfo::URI uri;
    uri.set_value(token.substr(0, pos));
    uri.set_executable(token.substr(pos + 1, 1) == "1");
    uri.set_extract(token.substr(pos + 2, 1) == "X");

    commandInfo.add_uris()->MergeFrom(uri);
  }

  CHECK(os::hasenv("MESOS_WORK_DIRECTORY"))
    << "Missing MESOS_WORK_DIRECTORY environment variable";
  std::string directory = os::getenv("MESOS_WORK_DIRECTORY");

  // We cannot use Some in the ternary expression because the compiler needs to
  // be able to infer the type, thus the explicit Option<string>.
  // TODO(idownes): Add an os::hasenv that returns an Option<string>.
  Option<std::string> user = os::hasenv("MESOS_USER")
    ? Option<std::string>(os::getenv("MESOS_USER")) // Explicit so it compiles.
    : None();

  // Fetch each URI to a local file, chmod, then chown if a user is provided.
  foreach (const CommandInfo::URI& uri, commandInfo.uris()) {
    // Fetch the URI to a local file.
    Try<string> fetched = fetch(fetchers, uri.value(), directory);
    if (fetched.isError()) {
      EXIT(1) << "Failed to fetch: " << uri.value();
    }

    // Chmod the fetched URI if it's executable, else assume it's an archive
    // that should be extracted.
    if (uri.executable()) {
      Try<Nothing> chmod = os::chmod(
          fetched.get(), S_IRWXU | S_IRGRP | S_IXGRP | S_IROTH | S_IXOTH);
      if (chmod.isError()) {
        EXIT(1) << "Failed to chmod " << fetched.get() << ": " << chmod.error();
      }
    } else if (uri.extract()) {
      //TODO(idownes): Consider removing the archive once extracted.
      // Try to extract the file if it's recognized as an archive.
      Try<bool> extracted = extract(fetched.get(), directory);
      if (extracted.isError()) {
        EXIT(1) << "Failed to extract "
                << fetched.get() << ":" << extracted.error();
      }
    } else {
      LOG(INFO) << "Skipped extracting path '" << fetched.get() << "'";
    }

    // Recursively chown the directory if a user is provided.
    if (user.isSome()) {
      Try<Nothing> chowned = os::chown(user.get(), directory);
      if (chowned.isError()) {
        EXIT(1) << "Failed to chown " << directory << ": " << chowned.error();
      }
    }
  }

  return 0;
}

/*
 *    Copyright (C) 2013 10gen Inc.
 *
 *    This program is free software: you can redistribute it and/or  modify
 *    it under the terms of the GNU Affero General Public License, version 3,
 *    as published by the Free Software Foundation.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    GNU Affero General Public License for more details.
 *
 *    You should have received a copy of the GNU Affero General Public License
 *    along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 *    As a special exception, the copyright holders give permission to link the
 *    code of portions of this program with the OpenSSL library under certain
 *    conditions as described in each individual source file and distribute
 *    linked combinations including the program with the OpenSSL library. You
 *    must comply with the GNU Affero General Public License in all respects
 *    for all of the code used other than as permitted herein. If you modify
 *    file(s) with this exception, you may extend this exception to your
 *    version of the file(s), but you are not obligated to do so. If you do not
 *    wish to do so, delete this exception statement from your version. If you
 *    delete this exception statement from all source files in the program,
 *    then also delete it in the license file.
 */

#include "mongo/s/mongos_options.h"

#include <string>
#include <vector>

#include "mongo/base/status.h"
#include "mongo/bson/util/builder.h"
#include "mongo/db/server_options.h"
#include "mongo/util/net/ssl_options.h"
#include "mongo/util/options_parser/option_description.h"
#include "mongo/util/options_parser/option_section.h"

namespace mongo {

    typedef moe::OptionDescription OD;
    typedef moe::PositionalOptionDescription POD;

    extern std::string dbpath;

    Status addMongosOptions(moe::OptionSection* options) {

        moe::OptionSection general_options("General options");

        Status ret = addGeneralServerOptions(&general_options);
        if (!ret.isOK()) {
            return ret;
        }

#if defined(_WIN32)
        moe::OptionSection windows_scm_options("Windows Service Control Manager options");

        ret = addWindowsServerOptions(&windows_scm_options);
        if (!ret.isOK()) {
            return ret;
        }
#endif

#ifdef MONGO_SSL
        moe::OptionSection ssl_options("SSL options");

        ret = addSSLServerOptions(&ssl_options);
        if (!ret.isOK()) {
            return ret;
        }
#endif

        moe::OptionSection sharding_options("Sharding options");

        sharding_options.addOptionChaining("sharding.configDB", "configdb", moe::String,
                "1 or 3 comma separated config servers");

        sharding_options.addOptionChaining("replication.localPingThresholdMs", "localThreshold",
                moe::Int, "ping time (in ms) for a node to be considered local (default 15ms)");

        sharding_options.addOptionChaining("test", "test", moe::Switch, "just run unit tests")
                                          .setSources(moe::SourceAllLegacy);

        sharding_options.addOptionChaining("upgrade", "upgrade", moe::Switch,
                "upgrade meta data version")
                                          .setSources(moe::SourceAllLegacy);

        sharding_options.addOptionChaining("chunkSize", "chunkSize", moe::Int,
                "maximum amount of data per chunk");

        sharding_options.addOptionChaining("net.ipv6", "ipv6", moe::Switch,
                "enable IPv6 support (disabled by default)");

        sharding_options.addOptionChaining("net.jsonp", "jsonp", moe::Switch,
                "allow JSONP access via http (has security implications)")
                                         .setSources(moe::SourceAllLegacy);

        sharding_options.addOptionChaining("noscripting", "noscripting", moe::Switch,
                "disable scripting engine")
                                         .setSources(moe::SourceAllLegacy);


        options->addSection(general_options);

#if defined(_WIN32)
        options->addSection(windows_scm_options);
#endif

        options->addSection(sharding_options);

#ifdef MONGO_SSL
        options->addSection(ssl_options);
#endif

        options->addOptionChaining("noAutoSplit", "noAutoSplit", moe::Switch,
                "do not send split commands with writes")
                                  .hidden()
                                  .setSources(moe::SourceAllLegacy);

        options->addOptionChaining("sharding.autoSplit", "", moe::Bool,
                "send split commands with writes")
                                  .setSources(moe::SourceYAMLConfig);


        return Status::OK();
    }

} // namespace mongo

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

        sharding_options.addOptionChaining("configdb", "configdb", moe::String,
                "1 or 3 comma separated config servers");

        sharding_options.addOptionChaining("localThreshold", "localThreshold", moe::Int,
                "ping time (in ms) for a node to be considered local (default 15ms)");

        sharding_options.addOptionChaining("test", "test", moe::Switch, "just run unit tests");

        sharding_options.addOptionChaining("upgrade", "upgrade", moe::Switch,
                "upgrade meta data version");

        sharding_options.addOptionChaining("chunkSize", "chunkSize", moe::Int,
                "maximum amount of data per chunk");

        sharding_options.addOptionChaining("ipv6", "ipv6", moe::Switch,
                "enable IPv6 support (disabled by default)");

        sharding_options.addOptionChaining("jsonp", "jsonp", moe::Switch,
                "allow JSONP access via http (has security implications)");

        sharding_options.addOptionChaining("noscripting", "noscripting", moe::Switch,
                "disable scripting engine");


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
                                  .hidden();


        return Status::OK();
    }

} // namespace mongo

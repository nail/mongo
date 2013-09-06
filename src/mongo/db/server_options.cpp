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
 *    must comply with the GNU Affero General Public License in all respects for
 *    all of the code used other than as permitted herein. If you modify file(s)
 *    with this exception, you may extend this exception to your version of the
 *    file(s), but you are not obligated to do so. If you do not wish to do so,
 *    delete this exception statement from your version. If you delete this
 *    exception statement from all source files in the program, then also delete
 *    it in the license file.
 */

#include "mongo/db/server_options.h"

#include "mongo/base/status.h"
#include "mongo/bson/util/builder.h"
#include "mongo/db/cmdline.h" // For CmdLine::DefaultDBPort
#include "mongo/util/net/listen.h" // For DEFAULT_MAX_CONN
#include "mongo/util/options_parser/option_description.h"
#include "mongo/util/options_parser/option_section.h"

namespace mongo {

    typedef moe::OptionDescription OD;
    typedef moe::PositionalOptionDescription POD;

    Status addGeneralServerOptions(moe::OptionSection* options) {
        StringBuilder portInfoBuilder;
        StringBuilder maxConnInfoBuilder;

        portInfoBuilder << "specify port number - " << CmdLine::DefaultDBPort << " by default";
        maxConnInfoBuilder << "max number of simultaneous connections - "
                           << DEFAULT_MAX_CONN << " by default";

        Status ret = options->addOption(OD("help", "help,h", moe::Switch,
                    "show this usage information", true));
        if (!ret.isOK()) {
            return ret;
        }
        ret = options->addOption(OD("version", "version", moe::Switch, "show version information",
                    true));
        if (!ret.isOK()) {
            return ret;
        }
        ret = options->addOption(OD("config", "config,f", moe::String,
                    "configuration file specifying additional options", true));
        if (!ret.isOK()) {
            return ret;
        }
        ret = options->addOption(OD("verbose", "verbose,v", moe::Switch,
                    "be more verbose (include multiple times for more verbosity e.g. -vvvvv)",
                    true));
        if (!ret.isOK()) {
            return ret;
        }
        ret = options->addOption(OD("quiet", "quiet", moe::Switch, "quieter output", true));
        if (!ret.isOK()) {
            return ret;
        }
        ret = options->addOption(OD("port", "port", moe::Int, portInfoBuilder.str().c_str(), true));
        if (!ret.isOK()) {
            return ret;
        }
        ret = options->addOption(OD("bind_ip", "bind_ip", moe::String,
                    "comma separated list of ip addresses to listen on - all local ips by default",
                    true));
        if (!ret.isOK()) {
            return ret;
        }
        ret = options->addOption(OD("maxConns", "maxConns", moe::Int,
                    maxConnInfoBuilder.str().c_str(), true));
        if (!ret.isOK()) {
            return ret;
        }
        ret = options->addOption(OD("logpath", "logpath", moe::String,
                    "log file to send write to instead of stdout - has to be a file, not directory",
                    true));
        if (!ret.isOK()) {
            return ret;
        }
        ret = options->addOption(OD("logappend", "logappend", moe::Switch,
                    "append to logpath instead of over-writing", true));
        if (!ret.isOK()) {
            return ret;
        }
        ret = options->addOption(OD("logTimestampFormat", "logTimestampFormat", moe::String,
                    "Desired format for timestamps in log messages. One of ctime, "
                    "iso8601-utc or iso8601-local", true));
        if (!ret.isOK()) {
            return ret;
        }
        ret = options->addOption(OD("pidfilepath", "pidfilepath", moe::String,
                    "full path to pidfile (if not set, no pidfile is created)", true));
        if (!ret.isOK()) {
            return ret;
        }
        ret = options->addOption(OD("keyFile", "keyFile", moe::String,
                    "private key for cluster authentication", true));
        if (!ret.isOK()) {
            return ret;
        }
        ret = options->addOption(OD("setParameter", "setParameter", moe::StringVector,
                    "Set a configurable parameter", true, moe::Value(), moe::Value(), true));
        if (!ret.isOK()) {
            return ret;
        }
        ret = options->addOption(OD("httpinterface", "httpinterface", moe::Switch,
                    "enable http interface", true));
        if (!ret.isOK()) {
            return ret;
        }
        ret = options->addOption(OD("clusterAuthMode", "clusterAuthMode", moe::String,
                    "Authentication mode used for cluster authentication. Alternatives are "
                    "(keyfile|sendKeyfile|sendX509|x509)", true));
        if (!ret.isOK()) {
            return ret;
        }
#ifndef _WIN32
        options->addOptionChaining("nounixsocket", "nounixsocket", moe::Switch,
                "disable listening on unix sockets");

        options->addOptionChaining("unixSocketPrefix", "unixSocketPrefix", moe::String,
                "alternative directory for UNIX domain sockets (defaults to /tmp)");

        options->addOptionChaining("fork", "fork", moe::Switch, "fork server process");

        options->addOptionChaining("syslog", "syslog", moe::Switch,
                "log to system's syslog facility instead of file or stdout");

#endif

        /* support for -vv -vvvv etc. */
        for (string s = "vv"; s.length() <= 12; s.append("v")) {
            options->addOptionChaining(s.c_str(), s.c_str(), moe::Switch, "verbose")
                                      .hidden();
        }

        // Extra hidden options
        options->addOptionChaining("nohttpinterface", "nohttpinterface", moe::Switch,
                "disable http interface")
                                  .hidden();

        options->addOptionChaining("objcheck", "objcheck", moe::Switch,
                "inspect client data for validity on receipt (DEFAULT)")
                                  .hidden();

        options->addOptionChaining("noobjcheck", "noobjcheck", moe::Switch,
                "do NOT inspect client data for validity on receipt")
                                  .hidden();

        options->addOptionChaining("traceExceptions", "traceExceptions", moe::Switch,
                "log stack traces for every exception")
                                  .hidden();

        options->addOptionChaining("enableExperimentalIndexStatsCmd",
                "enableExperimentalIndexStatsCmd", moe::Switch, "EXPERIMENTAL (UNSUPPORTED). "
                "Enable command computing aggregate statistics on indexes.")
                                  .hidden();

        options->addOptionChaining("enableExperimentalStorageDetailsCmd",
                "enableExperimentalStorageDetailsCmd", moe::Switch, "EXPERIMENTAL (UNSUPPORTED). "
                "Enable command computing aggregate statistics on storage.")
                                  .hidden();


        return Status::OK();
    }

    Status addWindowsServerOptions(moe::OptionSection* options) {
        options->addOptionChaining("install", "install", moe::Switch, "install Windows service");

        options->addOptionChaining("remove", "remove", moe::Switch, "remove Windows service");

        options->addOptionChaining("reinstall", "reinstall", moe::Switch,
                "reinstall Windows service (equivalent to --remove followed by --install)");

        options->addOptionChaining("serviceName", "serviceName", moe::String,
                "Windows service name");

        options->addOptionChaining("serviceDisplayName", "serviceDisplayName", moe::String,
                "Windows service display name");

        options->addOptionChaining("serviceDescription", "serviceDescription", moe::String,
                "Windows service description");

        options->addOptionChaining("serviceUser", "serviceUser", moe::String,
                "account for service execution");

        options->addOptionChaining("servicePassword", "servicePassword", moe::String,
                "password used to authenticate serviceUser");


        options->addOptionChaining("service", "service", moe::Switch, "start mongodb service")
                                  .hidden();


        return Status::OK();
    }

} // namespace mongo

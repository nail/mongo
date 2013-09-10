// cmdline.cpp

/**
*    Copyright (C) 2008 10gen Inc.
*    Copyright (C) 2013 Tokutek Inc.
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

#include <boost/algorithm/string.hpp>

#include "mongo/pch.h"

#include <boost/algorithm/string.hpp>

#include "mongo/db/cmdline.h"

#include "mongo/base/status.h"
#include "mongo/bson/util/builder.h"
#include "mongo/db/server_parameters.h"
#include "mongo/logger/message_event_utf8_encoder.h"
#include "mongo/util/map_util.h"
#include "mongo/util/mongoutils/str.h"
#include "mongo/util/net/listen.h"
#include "mongo/util/net/ssl_options.h"
#include "mongo/util/options_parser/environment.h"
#include "mongo/util/options_parser/option_section.h"
#include "mongo/util/options_parser/options_parser.h"
#include "mongo/util/password.h"

#ifdef _WIN32
#include <direct.h>
#endif

#define MAX_LINE_LENGTH 256

#include <fstream>

namespace po = boost::program_options;

// this is prototype for now, we'll see if it is helpful
namespace mongo {

    void assertStartingUp();

    /** "value is Const After Server Init" helper
    *
    * Example:
    *
    *  casi<int> foo = 3;
    *  foo.ref() = 4; // asserts if not still in server init
    *  int x = foo+1; // ok anytime
    *
    */
    template< class T >
    class casi : boost::noncopyable {
        T val;
    public:
        casi(const T& t) : val(t) { 
            DEV assertStartingUp();
        }
        operator const T& () { return val; }
        T& ref() { 
            DEV assertStartingUp();
            return val; 
        }
    };

    /** partially specialized for cases where out global variable is a pointer -- we want the value
     * pointed at to be constant, not just the pointer itself
     */
    template< typename T >
    class casi<T*> : boost::noncopyable {
        T * val;
        void operator=(T*);
    public:
        casi(T* t) : val(t) { 
            DEV assertStartingUp();
        }
        operator const T* () { return val; }
        const T* get() { return val; }
        T*& ref() { 
            DEV assertStartingUp();
            return val; 
        }
    };

} // namespace mongo

namespace mongo {

    static bool _isPasswordArgument(char const* argumentName);
    static bool _isPasswordSwitch(char const* switchName);

namespace {
    BSONArray argvArray;
    BSONObj parsedOpts;
}  // namespace

    BSONArray CmdLine::getArgvArray() {
        return argvArray;
    }

    BSONObj CmdLine::getParsedOpts() {
        return parsedOpts;
    }

    void CmdLine::addGlobalOptions( boost::program_options::options_description& general ,
                                    boost::program_options::options_description& hidden ,
                                    boost::program_options::options_description& ssl_options ) {
        /* support for -vv -vvvv etc. */
        for (string s = "vv"; s.length() <= 12; s.append("v")) {
            hidden.add_options()(s.c_str(), "verbose");
        }

        StringBuilder portInfoBuilder;
        StringBuilder maxConnInfoBuilder;

        portInfoBuilder << "specify port number - " << DefaultDBPort << " by default";
        maxConnInfoBuilder << "max number of simultaneous connections - " << DEFAULT_MAX_CONN << " by default";

        general.add_options()
        ("help,h", "show this usage information")
        ("version", "show version information")
        ("config,f", po::value<string>(), "configuration file specifying additional options")
        ("verbose,v", "be more verbose (include multiple times for more verbosity e.g. -vvvvv)")
        ("quiet", "quieter output")
        ("port", po::value<int>(), portInfoBuilder.str().c_str())
        ("bind_ip", po::value<string>(), "comma separated list of ip addresses to listen on - all local ips by default")
        ("maxConns",po::value<int>(), maxConnInfoBuilder.str().c_str())
        ("logpath", po::value<string>() , "log file to send write to instead of stdout - has to be a file, not directory" )
        ("logappend" , "append to logpath instead of over-writing" )
        ("logTimestampFormat", po::value<string>(), "Desired format for timestamps in log "
         "messages. One of ctime, iso8601-utc or iso8601-local")
        ("pidfilepath", po::value<string>(), "full path to pidfile (if not set, no pidfile is created)")
        ("keyFile", po::value<string>(), "private key for cluster authentication")
        ("setParameter", po::value< std::vector<std::string> >()->composing(),
                "Set a configurable parameter")
        ("httpinterface", "enable http interface")
        ("clusterAuthMode", po::value<std::string>(),
         "Authentication mode used for cluster authentication."
         " Alternatives are (keyfile|sendKeyfile|sendX509|x509)")
#ifndef _WIN32
        ("nounixsocket", "disable listening on unix sockets")
        ("unixSocketPrefix", po::value<string>(), "alternative directory for UNIX domain sockets (defaults to /tmp)")
        ("fork" , "fork server process" )
        ("syslog" , "log to system's syslog facility instead of file or stdout" )
#endif
        ("pluginsDir", po::value<string>(), "directory containing plugins (defaults to lib64/plugins)")
        ("loadPlugin", po::value<vector<string> >()->composing(), "load plugins at startup")
        ;
        

        if (argv.empty()) {
            return Status(ErrorCodes::InternalError, "Cannot get binary name: argv array is empty");
        }

        // setup binary name
        cmdLine.binaryName = argv[0];
        size_t i = cmdLine.binaryName.rfind( '/' );
        if ( i != string::npos ) {
            cmdLine.binaryName = cmdLine.binaryName.substr( i + 1 );
        }
        return Status::OK();
    }

    Status CmdLine::setupCwd() {
            // setup cwd
        char buffer[1024];
#ifdef _WIN32
        verify( _getcwd( buffer , 1000 ) );
#else
        verify( getcwd( buffer , 1000 ) );
#endif
        ;
        
        // Extra hidden options
        hidden.add_options()
        ("nohttpinterface", "disable http interface")
        ("objcheck", "inspect client data for validity on receipt (DEFAULT)")
        ("noobjcheck", "do NOT inspect client data for validity on receipt")
        ("traceExceptions", "log stack traces for every exception")
        ;
    }

    Status CmdLine::setArgvArray(const std::vector<std::string>& argv) {
        BSONArrayBuilder b;
        std::vector<std::string> censoredArgv = argv;
        censor(&censoredArgv);
        for (size_t i=0; i < censoredArgv.size(); i++) {
            b << censoredArgv[i];
        }
        argvArray = b.arr();
        return Status::OK();
    }

    void CmdLine::parseConfigFile( istream &f, stringstream &ss ) {
        string s;
        char line[MAX_LINE_LENGTH];

        while ( f.good() ) {
            f.getline(line, MAX_LINE_LENGTH);
            s = line;
            std::remove(s.begin(), s.end(), ' ');
            std::remove(s.begin(), s.end(), '\t');
            boost::to_upper(s);

            if ( s.find( "FASTSYNC" ) != string::npos )
                cout << "warning \"fastsync\" should not be put in your configuration file" << endl;

            // skip commented lines
            if ( s.c_str()[0] == '#' ) {
            // In this block, we copy the actual line into our intermediate buffer to actually be
            // parsed later only if the string does not contain the substring "=FALSE" OR the option
            // is a setParameter option.  Note that this is done after we call boost::to_upper
            // above.
            } else if ( s.find( "=FALSE" ) == string::npos ||
                        s.find( "SETPARAMETER" ) == 0 ) {
                ss << line << endl;
            } else {
                cerr << "warning: remove or comment out this line by starting it with \'#\', skipping now : " << line << endl;
            }
            else if (type == typeid(int))
                builder->append(key, value.as<int>());
            else if (type == typeid(double))
                builder->append(key, value.as<double>());
            else if (type == typeid(bool))
                builder->appendBool(key, value.as<bool>());
            else if (type == typeid(long))
                builder->appendNumber(key, (long long)value.as<long>());
            else if (type == typeid(unsigned))
                builder->appendNumber(key, (long long)value.as<unsigned>());
            else if (type == typeid(unsigned long long))
                builder->appendNumber(key, (long long)value.as<unsigned long long>());
            else if (type == typeid(vector<string>))
                builder->append(key, value.as<vector<string> >());
            else
                builder->append(key, "UNKNOWN TYPE: " + demangleName(type));
        }
        return Status::OK();
    }
} // namespace

    Status CmdLine::setParsedOpts(moe::Environment& params) {
        const std::map<moe::Key, moe::Value> paramsMap = params.getExplicitlySet();
        BSONObjBuilder builder;
        Status ret = valueMapToBSON(paramsMap, &builder);
        if (!ret.isOK()) {
            return ret;
        }
        return;
    }

    Status CmdLine::store( const std::vector<std::string>& argv,
                           moe::OptionSection& options,
                           moe::Environment& params ) {

        Status ret = CmdLine::setupBinaryName(argv);
        if (!ret.isOK()) {
            return ret;
        }

        ret = CmdLine::setupCwd();
        if (!ret.isOK()) {
            return ret;
        }

        moe::OptionsParser parser;

                stringstream ss;
                CmdLine::parseConfigFile( f, ss );
                po::store( po::parse_config_file( ss , all ) , params );
                f.close();
            }
        }

        ret = CmdLine::setArgvArray(argv);
        if (!ret.isOK()) {
            return ret;
        }

        {
            BSONArrayBuilder b;
            std::vector<std::string> censoredArgv = argv;
            censor(&censoredArgv);
            for (size_t i=0; i < censoredArgv.size(); i++) {
                b << censoredArgv[i];
            }
            argvArray = b.arr();
        }

        {
            BSONObjBuilder b;
            for (po::variables_map::const_iterator it(params.begin()), end(params.end()); it != end; it++){
                if (!it->second.defaulted()){
                    const string& key = it->first;
                    const po::variable_value& value = it->second;
                    const type_info& type = value.value().type();

                    if (type == typeid(string)){
                        if (value.as<string>().empty())
                            b.appendBool(key, true); // boost po uses empty string for flags like --quiet
                        else {
                            if ( _isPasswordArgument(key.c_str()) ) {
                                b.append( key, "<password>" );
                            }
                            else {
                                b.append( key, value.as<string>() );
                            }
                        }
                    }
                    else if (type == typeid(int))
                        b.append(key, value.as<int>());
                    else if (type == typeid(double))
                        b.append(key, value.as<double>());
                    else if (type == typeid(bool))
                        b.appendBool(key, value.as<bool>());
                    else if (type == typeid(long))
                        b.appendNumber(key, (long long)value.as<long>());
                    else if (type == typeid(unsigned))
                        b.appendNumber(key, (long long)value.as<unsigned>());
                    else if (type == typeid(unsigned long))
                        b.appendNumber(key, (unsigned long)value.as<unsigned long>());
                    else if (type == typeid(unsigned long long))
                        b.appendNumber(key, (long long)value.as<unsigned long long>());
                    else if (type == typeid(BytesQuantity<int>))
                        b.append(key, value.as<BytesQuantity<int> >());
                    else if (type == typeid(BytesQuantity<long>))
                        b.appendNumber(key, (long long)value.as<BytesQuantity<long> >());
                    else if (type == typeid(BytesQuantity<unsigned>))
                        b.appendNumber(key, (long long)value.as<BytesQuantity<unsigned> >());
                    else if (type == typeid(BytesQuantity<unsigned long>))
                        b.appendNumber(key, (unsigned long)value.as<BytesQuantity<unsigned long> >());
                    else if (type == typeid(BytesQuantity<unsigned long long>))
                        b.appendNumber(key, (long long)value.as<BytesQuantity<unsigned long long> >());
                    else if (type == typeid(vector<string>))
                        b.append(key, value.as<vector<string> >());
                    else
                        b.append(key, "UNKNOWN TYPE: " + demangleName(type));
                }
            }
            parsedOpts = b.obj();
        }

        if (params.count("verbose")) {
            logger::globalLogDomain()->setMinimumLoggedSeverity(logger::LogSeverity::Debug(1));
        }

        for (string s = "vv"; s.length() <= 12; s.append("v")) {
            if (params.count(s)) {
                logger::globalLogDomain()->setMinimumLoggedSeverity(
                        logger::LogSeverity::Debug(s.length()));
            }
        }

        if (params.count("enableExperimentalIndexStatsCmd")) {
            cmdLine.experimental.indexStatsCmdEnabled = true;
        }
        if (params.count("enableExperimentalStorageDetailsCmd")) {
            cmdLine.experimental.storageDetailsCmdEnabled = true;
        }

        if (params.count("port")) {
            cmdLine.port = params["port"].as<int>();
        }

        if (params.count("bind_ip")) {
            cmdLine.bind_ip = params["bind_ip"].as<std::string>();
        }

        if (params.count("clusterAuthMode")) {
            cmdLine.clusterAuthMode = params["clusterAuthMode"].as<std::string>();
        }

        if (params.count("quiet")) {
            cmdLine.quiet = true;
        }

        if (params.count("traceExceptions")) {
            DBException::traceExceptions = true;
        }

        if (params.count("maxConns")) {
            cmdLine.maxConns = params["maxConns"].as<int>();

            if ( cmdLine.maxConns < 5 ) {
                return Status(ErrorCodes::BadValue, "maxConns has to be at least 5");
            }
        }

        if (params.count("objcheck")) {
            cmdLine.objcheck = true;
        }
        if (params.count("noobjcheck")) {
            if (params.count("objcheck")) {
                return Status(ErrorCodes::BadValue, "can't have both --objcheck and --noobjcheck");
            }
            cmdLine.objcheck = false;
        }

        if (params.count("bind_ip")) {
            // passing in wildcard is the same as default behavior; remove and warn
            if ( cmdLine.bind_ip ==  "0.0.0.0" ) {
                cout << "warning: bind_ip of 0.0.0.0 is unnecessary; listens on all ips by default" << endl;
                cmdLine.bind_ip = "";
            }
        }

#ifndef _WIN32
        if (params.count("unixSocketPrefix")) {
            cmdLine.socket = params["unixSocketPrefix"].as<string>();
        }

        if (params.count("nounixsocket")) {
            cmdLine.noUnixSocket = true;
        }

        if (params.count("fork") && !params.count("shutdown")) {
            cmdLine.doFork = true;
        }
#endif  // _WIN32

        if (params.count("logTimestampFormat")) {
            using logger::MessageEventDetailsEncoder;
            std::string formatterName = params["logTimestampFormat"].as<string>();
            if (formatterName == "ctime") {
                MessageEventDetailsEncoder::setDateFormatter(dateToCtimeString);
            }
            else if (formatterName == "iso8601-utc") {
                MessageEventDetailsEncoder::setDateFormatter(dateToISOStringUTC);
            }
            else if (formatterName == "iso8601-local") {
                MessageEventDetailsEncoder::setDateFormatter(dateToISOStringLocal);
            }
            else {
                StringBuilder sb;
                sb << "Value of logTimestampFormat must be one of ctime, iso8601-utc " <<
                      "or iso8601-local; not \"" << formatterName << "\".";
                return Status(ErrorCodes::BadValue, sb.str());
            }
        }
        if (params.count("logpath")) {
            cmdLine.logpath = params["logpath"].as<string>();
            if (cmdLine.logpath.empty()) {
                return Status(ErrorCodes::BadValue, "logpath cannot be empty if supplied");
            }
        }

        if (params.count("gdb")) {
            cmdLine.gdb = true;
        }

        cmdLine.logWithSyslog = params.count("syslog");
        cmdLine.logAppend = params.count("logappend");
        if (!cmdLine.logpath.empty() && cmdLine.logWithSyslog) {
            return Status(ErrorCodes::BadValue, "Cant use both a logpath and syslog ");
        }

        if (cmdLine.doFork && cmdLine.logpath.empty() && !cmdLine.logWithSyslog) {
            return Status(ErrorCodes::BadValue, "--fork has to be used with --logpath or --syslog");
        }

        if (params.count("keyFile")) {
            cmdLine.keyFile = params["keyFile"].as<string>();
        }

        if ( params.count("pidfilepath")) {
            cmdLine.pidFile = params["pidfilepath"].as<string>();
        }

        if (params.count("pluginsDir")) {
            cmdLine.pluginsDir = params["pluginsDir"].as<string>();
        }

        if (params.count("loadPlugin")) {
            const vector<string> &plugins = params["loadPlugin"].as<vector<string> >();
            cmdLine.plugins.insert(cmdLine.plugins.end(), plugins.begin(), plugins.end());
        }

        if (params.count("setParameter")) {
            std::vector<std::string> parameters =
                params["setParameter"].as<std::vector<std::string> >();
            for (size_t i = 0, length = parameters.size(); i < length; ++i) {
                std::string name;
                std::string value;
                if (!mongoutils::str::splitOn(parameters[i], '=', name, value)) {
                    StringBuilder sb;
                    sb << "Illegal option assignment: \"" << parameters[i] << "\"";
                    return Status(ErrorCodes::BadValue, sb.str());
                }
                ServerParameter* parameter = mapFindWithDefault(
                        ServerParameterSet::getGlobal()->getMap(),
                        name,
                        static_cast<ServerParameter*>(NULL));
                if (NULL == parameter) {
                    StringBuilder sb;
                    sb << "Illegal --setParameter parameter: \"" << name << "\"";
                    return Status(ErrorCodes::BadValue, sb.str());
                }
                if (!parameter->allowedToChangeAtStartup()) {
                    StringBuilder sb;
                    sb << "Cannot use --setParameter to set \"" << name << "\" at startup";
                    return Status(ErrorCodes::BadValue, sb.str());
                }
                Status status = parameter->setFromString(value);
                if (!status.isOK()) {
                    StringBuilder sb;
                    sb << "Bad value for parameter \"" << name << "\": " << status.reason();
                    return Status(ErrorCodes::BadValue, sb.str());
                }
            }
        }
        if (!params.count("clusterAuthMode")){
            cmdLine.clusterAuthMode = "keyfile";
        }

#ifdef MONGO_SSL

        if (params.count("ssl.PEMKeyFile")) {
            cmdLine.sslPEMKeyFile = params["ssl.PEMKeyFile"].as<string>();
        }

        if (params.count("ssl.PEMKeyPassword")) {
            cmdLine.sslPEMKeyPassword = params["ssl.PEMKeyPassword"].as<string>();
        }

        if (params.count("ssl.clusterFile")) {
            cmdLine.sslClusterFile = params["ssl.clusterFile"].as<string>();
        }

        if (params.count("ssl.clusterPassword")) {
            cmdLine.sslClusterPassword = params["ssl.clusterPassword"].as<string>();
        }

        if (params.count("ssl.CAFile")) {
            cmdLine.sslCAFile = params["ssl.CAFile"].as<std::string>();
        }

        if (params.count("ssl.CRLFile")) {
            cmdLine.sslCRLFile = params["ssl.CRLFile"].as<std::string>();
        }

        if (params.count("ssl.weakCertificateValidation")) {
            cmdLine.sslWeakCertificateValidation = true;
        }
        if (params.count("ssl.sslOnNormalPorts")) {
            cmdLine.sslOnNormalPorts = true;
            if ( cmdLine.sslPEMKeyFile.size() == 0 ) {
                log() << "need sslPEMKeyFile" << endl;
                return false;
            }
            if (cmdLine.sslWeakCertificateValidation &&
                cmdLine.sslCAFile.empty()) {
                return Status(ErrorCodes::BadValue,
                              "need sslCAFile with sslWeakCertificateValidation");
            }
            if (params.count("sslFIPSMode")) {
                cmdLine.sslFIPSMode = true;
            }
        }
        else if (cmdLine.sslPEMKeyFile.size() || 
                 cmdLine.sslPEMKeyPassword.size() ||
                 cmdLine.sslClusterFile.size() ||
                 cmdLine.sslClusterPassword.size() ||
                 cmdLine.sslCAFile.size() ||
                 cmdLine.sslCRLFile.size() ||
                 cmdLine.sslWeakCertificateValidation ||
                 cmdLine.sslFIPSMode) {
            return Status(ErrorCodes::BadValue, "need to enable sslOnNormalPorts");
        }
        if (cmdLine.clusterAuthMode == "sendKeyfile" || 
            cmdLine.clusterAuthMode == "sendX509" || 
            cmdLine.clusterAuthMode == "x509") {
            if (!cmdLine.sslOnNormalPorts){
                return Status(ErrorCodes::BadValue, "need to enable sslOnNormalPorts");
            }
        }
        else if (cmdLine.clusterAuthMode != "keyfile") {
            log() << "unsupported value for clusterAuthMode " << cmdLine.clusterAuthMode << endl;
            return false;
        }
#else // ifdef MONGO_SSL
        // Keyfile is currently the only supported value if not using SSL 
        if (cmdLine.clusterAuthMode != "keyfile") {
            log() << "unsupported value for clusterAuthMode " << cmdLine.clusterAuthMode << endl;
            return false;
        }
#endif

        return Status::OK();
    }

    static bool _isPasswordArgument(const char* argumentName) {
        static const char* const passwordArguments[] = {
            "sslPEMKeyPassword",
            "ssl.PEMKeyPassword",
            "servicePassword",
            NULL  // Last entry sentinel.
        };
        for (const char* const* current = passwordArguments; *current; ++current) {
            if (mongoutils::str::equals(argumentName, *current))
                return true;
        }
        return false;
    }

    static bool _isPasswordSwitch(const char* switchName) {
        if (switchName[0] != '-')
            return false;
        size_t i = 1;
        if (switchName[1] == '-')
            i = 2;
        switchName += i;

        return _isPasswordArgument(switchName);
    }

    static void _redact(char* arg) {
        for (; *arg; ++arg)
            *arg = 'x';
    }

    void CmdLine::censor(std::vector<std::string>* args) {
        for (size_t i = 0; i < args->size(); ++i) {
            std::string& arg = args->at(i);
            const std::string::iterator endSwitch = std::find(arg.begin(), arg.end(), '=');
            std::string switchName(arg.begin(), endSwitch);
            if (_isPasswordSwitch(switchName.c_str())) {
                if (endSwitch == arg.end()) {
                    if (i + 1 < args->size()) {
                        args->at(i + 1) = "<password>";
                    }
                }
                else {
                    arg = switchName + "=<password>";
                }
            }
        }
    }

    void CmdLine::censor(int argc, char** argv) {
        // Algorithm:  For each arg in argv:
        //   Look for an equal sign in arg; if there is one, temporarily nul it out.
        //   check to see if arg is a password switch.  If so, overwrite the value
        //     component with xs.
        //   restore the nul'd out equal sign, if any.
        for (int i = 0; i < argc; ++i) {

            char* const arg = argv[i];
            char* const firstEqSign = strchr(arg, '=');
            if (NULL != firstEqSign) {
                *firstEqSign = '\0';
            }

            if (_isPasswordSwitch(arg)) {
                if (NULL == firstEqSign) {
                    if (i + 1 < argc) {
                        _redact(argv[i + 1]);
                    }
                }
                else {
                    _redact(firstEqSign + 1);
                }
            }

            if (NULL != firstEqSign) {
                *firstEqSign = '=';
            }
        }
    }

    void printCommandLineOpts() {
        log() << "options: " << parsedOpts << endl;
    }
}

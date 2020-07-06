#include "CalConst.h"

/**
 * CalTransaction status code standard.
 *
 * For normal termination of a CalTransaction, the status code can simply be set to
 * CAL::TRANS_OK
 * For error termination, the format of the status code is as follow:
 *
 * <severity>.<module name>.<system error code>.<module return code>
 *
 */

// CalTransaction Types
// Predefined CAL Transactions TYPES, DON't REUSE these unless you are instrumenting
// new server or web application
const std::string CAL::TRANS_TYPE_CLIENT = std::string("CLIENT");
const std::string CAL::TRANS_TYPE_EXEC = std::string("EXEC");
const std::string CAL::TRANS_TYPE_FETCH = std::string("FETCH");
const std::string CAL::TRANS_TYPE_FETCH_F = std::string("FETCHF");
const std::string CAL::TRANS_TYPE_FETCH_BATCH = std::string("FETCHB");
const std::string CAL::TRANS_TYPE_URL = std::string("URL");
const std::string CAL::TRANS_TYPE_API = std::string("API");
const std::string CAL::TRANS_TYPE_REPLAY = std::string("REPLAY");

// Add any other Transaction TYPES which you might defined for your application
const std::string CAL::TRANS_TYPE_DCC_WEBBUG = std::string("DCC_WEBBUG");
const std::string CAL::TRANS_TYPE_IEFT_PROC = std::string("IEFT_PROC");
const std::string CAL::TRANS_TYPE_IEFT_SF = std::string("IEFT_SF");
const std::string CAL::TRANS_TYPE_AUTH_SETTLE = std::string("AUTH_SETTLE");
const std::string CAL::TRANS_TYPE_AUTH_PATH_TWO = std::string("AUTH_PATH_2");
const std::string CAL::TRANS_TYPE_PARTNER_ONBOARD = std::string("PARTNER_ONBOARD");
const std::string CAL::TRANS_TYPE_ATTACK_CLIENT = std::string("ATTACK_CLIENT");
const std::string CAL::TRANS_TYPE_MF_BATCH_D = std::string("MF_BATCH_D");

// severity code, Don't add extra level of SEVERITY
const std::string CAL::TRANS_OK = std::string("0");
const std::string CAL::TRANS_FATAL = std::string("1");
const std::string CAL::TRANS_ERROR = std::string("2");
const std::string CAL::TRANS_WARNING = std::string("3");

// Addition data field name
const std::string CAL::ERR_DESCRIPTION = std::string("ERR_DESCRIPTION");
const std::string CAL::ERR_ACTION = std::string("ERR_ACTION");

// Module names
const std::string CAL::MOD_NONE = std::string("");
const std::string CAL::MOD_ADMIN = std::string("ADMIN");
const std::string CAL::MOD_OCC = std::string("OCC");
const std::string CAL::MOD_PIMP = std::string("PIMP");
const std::string CAL::MOD_GENERIC_CLIENT = std::string("CLIENT");
const std::string CAL::MOD_GENERIC_SERVER = std::string("SERVER");
const std::string CAL::MOD_TH = std::string("TH");
const std::string CAL::MOD_TRANSUTIL = std::string("TRANSUTIL");
const std::string CAL::MOD_WEBSCR = std::string("WEBSCR");
const std::string CAL::MOD_CRYPTO = std::string("CRYPTO");
const std::string CAL::MOD_BATCH = std::string("BATCH");
const std::string CAL::MOD_FRAUDUTIL = std::string("FRAUDUTIL");
const std::string CAL::MOD_XML_INTERFACE_EBAY = std::string("XML_INTERFACE_EBAY");

// Transaction names
const std::string CAL::TRANS_NAME_DCC_REGISTER = std::string("Register");
const std::string CAL::TRANS_NAME_DCC_REGISTER_CHECK = std::string("RegisterCheck");
const std::string CAL::TRANS_NAME_WAX_REGISTER = std::string("WAXRegister");

// System error codes
const std::string CAL::SYS_ERR_NONE = std::string("");
const std::string CAL::SYS_ERR_2PC = std::string("2PC");
const std::string CAL::SYS_ERR_ACCESS_DENIED = std::string("ACCESS DENIED");
const std::string CAL::SYS_ERR_ATTACKSERV = std::string("ATTACKSERV");
const std::string CAL::SYS_ERR_CONFIG = std::string("CONFIG");
const std::string CAL::SYS_ERR_CONNECTION_FAILED = std::string("CONNECTION_FAILED");
const std::string CAL::SYS_ERR_CRYPTOSERV = std::string("CRYPTOSERV");
const std::string CAL::SYS_ERR_DATA = std::string("DATA");
const std::string CAL::SYS_ERR_EMAIL = std::string("EMAIL");
const std::string CAL::SYS_ERR_HANDSHAKE = std::string("HANDSHAKE");
const std::string CAL::SYS_ERR_INQUIRA = std::string("INQUIRA");
const std::string CAL::SYS_ERR_INTERNAL = std::string("INTERNAL");
const std::string CAL::SYS_ERR_IPN = std::string("IPN");
const std::string CAL::SYS_ERR_MARKED_DOWN = std::string("MARKED DOWN");
const std::string CAL::SYS_ERR_OCC = std::string("OCC");
const std::string CAL::SYS_ERR_ORACLE = std::string("ORACLE");
const std::string CAL::SYS_ERR_RT_RECON = std::string("REALTIME_RECON");
const std::string CAL::SYS_ERR_SQL = std::string("SQL");
const std::string CAL::SYS_ERR_AMQ = std::string("AMQ");
const std::string CAL::SYS_ERR_UNKNOWN = std::string("UNKNOWN");

// Event types
const std::string CAL::EVENT_TYPE_FATAL = std::string("FATAL");
const std::string CAL::EVENT_TYPE_ERROR = std::string("ERROR");
const std::string CAL::EVENT_TYPE_WARNING = std::string("WARNING");
const std::string CAL::EVENT_TYPE_EXCEPTION = std::string("EXCEPTION");
const std::string CAL::EVENT_TYPE_CLIENTINFO = std::string("ClientInfo");
const std::string CAL::EVENT_TYPE_BACKTRACE = std::string("Backtrace");
const std::string CAL::EVENT_TYPE_PAYLOAD = std::string("Payload");
const std::string CAL::EVENT_TYPE_MARKUP = std::string("MarkUp");
const std::string CAL::EVENT_TYPE_MARKDOWN = std::string("MarkDown");
const std::string CAL::EVENT_TYPE_TL = std::string("TL");
const std::string CAL::EVENT_TYPE_EOA = std::string("EOA");
const std::string CAL::EVENT_TYPE_MESSAGE = std::string("MSG");
const std::string CAL::EVENT_TYPE_ATTACKCLIENT = std::string("ATTACKCLIENT");

//The name used by infrastructure to log client related information not to be used in product code.
const std::string CAL::EVENT_TYPE_CLIENT_INFO = std::string("CLIENT_INFO");
const std::string CAL::EVENT_TYPE_SERVER_INFO = std::string("SERVER_INFO");

//All cal messages for business monitoring should be done with this event type. It will be mainly used by product code.
const std::string CAL::EVENT_TYPE_BIZ = std::string("BIZ");

const std::string CAL::TRANS_TYPE_CLIENT_SESSION = std::string("CLIENT_SESSION"); 

// Failure status 
const std::string CAL::SYSTEM_FAILURE = std::string("1");
const std::string CAL::INPUT_FAILURE = std::string("2");

std::string CAL::get_trans_ok() { return std::string("0"); }

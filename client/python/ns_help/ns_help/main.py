'''
Parses a netstring into a python list of tuples
'''

def get_netstrings (in_string):
    """TODO"""
    raise "Not implemented on this platform"

try:
    import _ns_help
    get_netstrings = _ns_help.get_netstrings

except ImportError as e:
    # Errors that can occur during netstring parsing
    NETSTRING_ERROR_TOO_LONG = 1
    NETSTRING_ERROR_NO_COLON = 2
    NETSTRING_ERROR_TOO_SHORT = 3
    NETSTRING_ERROR_NO_COMMA = 4
    NETSTRING_ERROR_LEADING_ZERO = 5
    NETSTRING_ERROR_NO_LENGTH = 6
    NETSTRING_ERROR_CODE_WRONG = 7
    _errs = ["NA",
             "String longer than 999999999 bytes",
             "String missing colon",
             "String shorter than length advertised",
             "String missing comma",
             "String beginning with zero",
             "String without length prefix",
             "String with malformed payload code"
             ]
             
    def get_netstrings(in_string):
        res = []
        while len(in_string) > 3:
            if len(in_string) > 999999999:
                return None, None, None, _errs[NETSTRING_ERROR_TOO_LONG]
            if len(in_string) < 3:
                return None, None, None, _errs[NETSTRING_ERROR_TOO_SHORT]
            if in_string[0:1] == b'0' and in_string[1:2].isdigit():
                return None, None, _errs[NETSTRING_ERROR_LEADING_ZERO]
            if not in_string[0:1].isdigit():
                return None, None, None, _errs[NETSTRING_ERROR_NO_LENGTH]
            pos = 0
            val = 0
            while pos < len(in_string) and in_string[pos:pos+1] != b':':
                if not in_string[pos:pos+1].isdigit():
                    return None, None, None, _errs[NETSTRING_ERROR_CODE_WRONG]

                pos = pos + 1
                #  Error if more than 9 digits
                if pos >= 9:
                    return None, None, None, _errs[NETSTRING_ERROR_TOO_LONG]
            val = int(in_string[0:pos])
            if pos + val + 1 >= len(in_string):
                return None, None, None, _errs[NETSTRING_ERROR_TOO_SHORT]
            if in_string[pos:pos+1] != b':':
                return None, None, None, _errs[NETSTRING_ERROR_NO_COLON]
            pos = pos + 1
            if in_string[pos + val:pos+val+1] != b',':
                return None, _errs[NETSTRING_ERROR_NO_COMMA]
            init_pos = pos
            while pos + init_pos < len(in_string) and in_string[pos:pos+1] != b' ' and in_string[pos:pos+1] != b',':
                if not in_string[pos:pos+1].isdigit():
                    return None, None, None, _errs[NETSTRING_ERROR_CODE_WRONG]

                pos = pos + 1
                # Error if more than 9 digits
                if pos >= 9:
                    return None, None, None, _errs[NETSTRING_ERROR_TOO_LONG]
            code = int(in_string[init_pos:pos])
            if in_string[pos] == b",":
                payload = None
                code = None
            else:
                payload = in_string[pos + 1:pos - 1 + val]
            res.append((code, payload))
            in_string = in_string[pos - 1 + val + 1:]
        return res

def handle_test_data(test_data):
    nss = get_netstrings(test_data)
    for a in nss:
        code, ns = a
        print ("get " + str(code) + " payload " + str(ns))

    

def main():
    test_data = b"""8:3 118040,3:3 L,60:3 Inbound file registered as expected by scripting interface,12:3 1284532945,6:3 1182,1:3,1:3,1:3,3:3 ?,5:3 422,36:3 p20100914_star_bin_file_1_test.txt,1:3,3:3 0,12:3 1284447600,8:3 118040,3:3 L,75:3 FPSM Manager updates Filename and External Filename with advanced tokens.,12:3 1284532945,6:3 1182,1:3,1:3,1:3,3:3 ?,5:3 422,36:3 p20100914_star_bin_file_1_test.txt,1:3,3:3 0,12:3 1284447600,8:3 118047,3:3 L,60:3 Inbound file registered as expected by scripting interface,12:3 1284539513,6:3 1206,1:3,1:3,1:3,3:3 ?,5:3 422,36:3 p20100915_star_bin_file_3_test.txt,1:3,3:3 3,12:3 1284534000,8:3 118047,3:3 L,75:3 FPSM Manager updates Filename and External Filename with advanced tokens.,12:3 1284539513,6:3 1206,1:3,1:3,1:3,3:3 ?,5:3 422,36:3 p20100915_star_bin_file_3_test.txt,1:3,3:3 3,12:3 1284534000,8:3 118047,3:3 H,29:3 GenericLoader file loading.,12:3 1284539540,6:3 1206,1:3,1:3,1:3,3:3 ?,5:3 422,36:3 p20100915_star_bin_file_3_test.txt,1:3,3:3 3,12:3 1284534000,8:3 118047,3:3 H,36:3 Meta data updated by FPSM manager.,12:3 1284539541,6:3 1206,1:3,1:3,1:3,3:3 ?,5:3 422,36:3 p20100915_star_bin_file_3_test.txt,1:3,3:3 3,12:3 1284534000,8:3 118047,3:3 F,29:3 GenericLoader file loading.,12:3 1284539541,6:3 1206,1:3,1:3,1:3,3:3 ?,5:3 422,36:3 p20100915_star_bin_file_3_test.txt,1:3,3:3 3,12:3 1284534000,8:3 118047,3:3 T,42:3 Processed by platform service framework.,12:3 1284539523,6:3 1206,1:3,1:3,1:3,3:3 ?,5:3 422,36:3 p20100915_star_bin_file_3_test.txt,1:3,3:3 3,12:3 1284534000,8:3 118047,3:3 E,42:3 Processed by platform service framework.,12:3 1284539523,6:3 1206,1:3,1:3,1:3,3:3 ?,5:3 422,36:3 p20100915_star_bin_file_3_test.txt,1:3,3:3 3,12:3 1284534000,8:3 118050,3:3 L,60:3 Inbound file registered as expected by scripting interface,12:3 1284540093,6:3 1222,1:3,1:3,1:3,3:3 ?,5:3 426,44:3 p20100915_star_ewallet_bin_file_4_test.txt,1:3,3:3 0,12:3 1284534000,8:3 118050,3:3 L,75:3 FPSM Manager updates Filename and External Filename with advanced tokens.,12:3 1284540093,6:3 1222,1:3,1:3,1:3,3:3 ?,5:3 426,44:3 p20100915_star_ewallet_bin_file_4_test.txt,1:3,3:3 0,12:3 1284534000,8:3 118050,3:3 E,6:3 test,12:3 1284540188,6:3 1222,1:3,1:3,1:3,3:3 ?,5:3 426,44:3 p20100915_star_ewallet_bin_file_4_test.txt,1:3,3:3 0,12:3 1284534000,8:3 118051,3:3 L,60:3 Inbound file registered as expected by scripting interface,12:3 1284720562,6:3 1229,1:3,1:3,1:3,3:3 ?,5:3 422,36:3 p20100917_star_bin_file_1_test.txt,1:3,3:3 0,12:3 1284706800,8:3 118051,3:3 L,75:3 FPSM Manager updates Filename and External Filename with advanced tokens.,12:3 1284720563,6:3 1229,1:3,1:3,1:3,3:3 ?,5:3 422,36:3 p20100917_star_bin_file_1_test.txt,1:3,3:3 0,12:3 1284706800,8:3 118051,3:3 M,6:3 test,12:3 1284720783,6:3 1229,1:3,1:3,1:3,3:3 ?,5:3 422,36:3 p20100917_star_bin_file_1_test.txt,1:3,3:3 0,12:3 1284706800,8:3 118422,3:3 L,60:3 Inbound file registered as expected by scripting interface,12:3 1308962363,6:3 2559,1:3,1:3,1:3,3:3 F,1:3,26:3 20110624_eft_input_2.txt,1:3,3:3 1,12:3 1308962331,8:3 118422,3:3 L,75:3 FPSM Manager updates Filename and External Filename with advanced tokens.,12:3 1308962363,6:3 2559,1:3,1:3,1:3,3:3 F,1:3,26:3 20110624_eft_input_2.txt,1:3,3:3 1,12:3 1308962331,8:3 118422,3:3 E,61:3 Inbound file registered as received by scripting interface.,12:3 1308962380,6:3 2559,1:3,1:3,1:3,3:3 F,1:3,26:3 20110624_eft_input_2.txt,1:3,3:3 1,12:3 1308962331,8:3 118422,3:3 H,29:3 GenericLoader file loading.,12:3 1308962425,6:3 2559,1:3,1:3,1:3,3:3 F,1:3,26:3 20110624_eft_input_2.txt,1:3,3:3 1,12:3 1308962331,8:3 118422,3:3 H,21:3 Update record count,12:3 1308962426,6:3 2559,1:3,1:3,1:3,3:3 F,1:3,26:3 20110624_eft_input_2.txt,1:3,3:3 1,12:3 1308962331,"""
    handle_test_data(test_data)
    handle_test_data(b"3:3 4,3:3 0,")
    

if __name__ == "__main__":
    main()
    

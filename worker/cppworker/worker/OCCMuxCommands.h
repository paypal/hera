#ifndef _OCCMUX_COMMANDS_H
#define _OCCMUX_COMMANDS_H

enum ServerInternalCommand {
	CMD_CONTROL_MSG		=501,
	CMD_EOR				=502, // end of response
	CMD_INTERRUPT_MSG	=503,
};

#endif // _OCCMUX_COMMANDS_H

package com.paypal.hera.cal;

/**
 *
 * All the static methods associated with writing Events
 *
 */
public class CalEventHelper {
	private CalEventHelper() {}
	
	/**
	 *
	 * @param type a case sensitive character string. .  For events, types should be one of the following: "Info", "Warn", "Error", and "CAL", where "CAL" is used for CAL internal messages.  NOTE: Only the first 8 characters of a type string are significant
	 * @param name a case sensitive character string whose namespace is within a type.  Names for URL transaction type are "ViewItem", "MakeBid", "SignIn" etc; names for SQL transaction type are comments such as "AddFeedback", "GetAccountDetailUnti_4", "IncrementFeedbackAndTotalScore" etc.
	 * @param status a case sensitive character string whose namespace may be global.  When a status is set repeatedly, the first non-"0" is used.
	 * @param data a list of name=value pairs separated by '&amp;'.
	 */
	public static void sendImmediate(String type, String name, String status, String data) {
		//InternalCalEventHelper.sendImmediate(type, name, status, data);
	}
	/**
	 * Write Warning Event
	 *
	 * @param eventName - short string for the name of the CalEvent(e.g.
	 * who or where the event occurs)
	 * @param e - non-fatal exception
	 * @param message - additional information
	 */
	public static void writeWarning(final String eventName, final Throwable e, final String message) {
		//writeLog(CalEvent.CAL_WARNING, eventName, message, e, true);
	}
	/**
	 * Write Warning Event
	 *
	 * @param eventName - short string for the name of the CalEvent(e.g.
	 * who or where the event occurs)
	 * @param e - non-fatal exception
	 */
	public static void writeWarning(final String eventName, final Throwable e) {
		//writeLog(CalEvent.CAL_WARNING, eventName, null, e, true);
	}
	/**
	 * Write Warning Event
	 *
	 * @param eventName - short string for the name of the CalEvent(e.g.
	 * who or where the event occurs)
	 *
	 * @param e - exception
	 * @param dumpStack - if stack trace wanted
	 * @param message - additional information

	 */
	public static void writeWarning(final String eventName, final Throwable e, final boolean dumpStack,
		final String message) {
		//writeLog(CalEvent.CAL_WARNING, eventName, message, e, dumpStack);
	}
	/**
	 * Write Exception Event
	 *
	 * @param eventName - short string for the name of the CalEvent(e.g.
	 * who or where the event occurs)
	 * @param e - exception
	 * @param message - additional information
	 */
	public static void writeException(final String eventName, final Throwable e, final String message) {
		//writeLog(CalEvent.CAL_EXCEPTION, eventName, message, e, true);
	}

	/**
	 * Write Exception Event
	 *
	 * @param eventName - short string for the name of the CalEvent(e.g.
	 * who or where the event occurs)
	 *
	 * @param e - exception
	 * @param partialDumpStack - if stack trace wanted
	 */
	public static void writeException(final String eventName, final Throwable e, final boolean partialDumpStack) {
		//InternalCalEventHelper.writeException(eventName, e, partialDumpStack);
	}
	
	/**
	 * Write Exception Event
	 *
	 * @param eventName - short string for the name of the CalEvent(e.g.
	 * who or where the event occurs)
	 *
	 * @param e - exception
	 * @param dumpStack - if stack trace wanted
	 * @param message - additional information

	 */
	public static void writeException(final String eventName, final Throwable e, final boolean dumpStack,
		final String message) {
		//writeLog(CalEvent.CAL_EXCEPTION, eventName, message, e, dumpStack);
	}
	/**
	 * Write Exception Event
	 *
	 * @param eventName - short string for the name of the CalEvent(e.g.
	 * who or where the event occurs)
	 * @param e - exception
	 */
	public static void writeException(String eventName, Throwable e) {
		//writeLog(CalEvent.CAL_EXCEPTION, eventName, null, e, true);
	}

	public static void writeLog(String eventType, String eventName,
		String message, String status)
	{
		writeLog(eventType, eventName, message, null, true, status);
	}

	public static void writeLog(String eventType, String eventName,
		String message, Throwable e, String statusString)
	{
		writeLog(eventType, eventName, message, e, true, statusString);
	}

	public static void writeLog(String eventType, String eventName,
		String message, Throwable e, boolean dumpStack)
	{
		writeLog(eventType, eventName, message, e, dumpStack, null);
	}
	
	/**
	 * Write Log Event
	 *
	 * @param eventType - the event type
	 * @param eventName - short string for the name of the CalEventImpl(e.g.  who or where the event occurs)
	 * @param message - additional information
	 * @param e - exception
	 * @param dumpStack - indicates if a stack dump is wanted
	 * @param statusString - the status for event.
	 */
	public static void writeLog(String eventType, String eventName,
		String message, Throwable e, boolean dumpStack, String statusString)
	{
		//InternalCalEventHelper.writeLog(eventType, eventName, message, e, dumpStack, statusString);
	}
}

package com.paypal.hera.dal.cm.wrapper;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import com.paypal.hera.dal.jdbc.rt.JdbcOperationType;

/**
 * Keeps track of child objects and closes them when this wrapper is closed
 * 
 */
public class CmBaseWrapper {

	private CmBaseWrapper m_parent;
	private List m_children;
	private boolean m_toBeDestroyed;
	private boolean m_isClosed;
	private boolean m_isInUse;
	private JdbcOperationType m_lastOpType;

	/**
	 * Create a new root DelegatingProxy
	 */
	CmBaseWrapper() {
		m_parent = null;
	}

	/**
	 * Create a new DelegatingProxy with parent
	 */
	CmBaseWrapper(CmBaseWrapper parent) {
		m_parent = parent;
		if (parent != null) {
			parent.addChild(this);
		}
	}

	private void addChild(CmBaseWrapper child) {
		synchronized (getLock()) {
			if (m_children == null) {
				m_children = new ArrayList();
			}
			m_children.add(child);
		}
	}

	protected void close() throws SQLException {
		if (m_isClosed) {
			return;
		}

		m_isClosed = true;
		List children = m_children;
		m_children = null;

		if (children != null) {
			Exception childException = null;

			for (int i=0; i<children.size(); i++) {
				CmBaseWrapper child = (CmBaseWrapper)
					children.get(i);
				try {
					child.parentClosed();
				} catch (Exception e) {
					if (childException == null) {
						childException = e;
					}
				}
			}

			if (childException != null) {
				// @PMD:REVIEWED:AvoidDeeplyNestedIfStmts: by ichernyshev on 09/02/05
				if (childException instanceof SQLException) {
					throw (SQLException)childException;
				}

				throw new SQLException(getClassName() +
					": Unable to close child: " + childException.toString());
			}
		}

		// TODO: do we want to remove ourselves from parent?

		if (m_isInUse) {
			throw new SQLException(getClassName() + " is closed while in use");
		}
	}

	protected void parentClosed()
		throws SQLException
	{
	}

	protected String getClassName() {
		return getClass().getName();
	}

	protected final void checkOpened() throws SQLException {
		if (m_isClosed) {
			throw new CmWrapperClosedException(getClassName() + " is closed");
		}
	}

	public boolean isClosed() throws SQLException
	{
		return m_isClosed;
	}

	public void markToBeDestroyed() {
		m_toBeDestroyed = true;
	}

	public boolean shouldBeDestroyed() {
		return m_toBeDestroyed;
	}

	public boolean isInUse() {
		return m_isInUse;
	}

	protected void startUse(JdbcOperationType opType) {
		m_isInUse = true;
		m_lastOpType = opType;
	}

	protected void endUse(JdbcOperationType opType)
	{
		//JdbcOperationType prevOpType = m_lastOpType;

		m_isInUse = false;
		m_lastOpType = null;

		//if (prevOpType != opType) {
			// TODO: log error
		//}
	}

	protected Object getLock() {
		return m_parent.getLock();
	}
}

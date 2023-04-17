package com.paypal.hera.parser.vistors;

import com.paypal.hera.parser.sqlmetadata.InsertItemMetaData;
import com.paypal.hera.parser.sqlmetadata.MetaDataConstant;
import com.paypal.hera.parser.sqlmetadata.SQLMetaData;
import com.paypal.hera.parser.sqlmetadata.TableDetails;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.*;
import net.sf.jsqlparser.statement.alter.Alter;
import net.sf.jsqlparser.statement.alter.AlterSession;
import net.sf.jsqlparser.statement.alter.AlterSystemStatement;
import net.sf.jsqlparser.statement.alter.RenameTableStatement;
import net.sf.jsqlparser.statement.alter.sequence.AlterSequence;
import net.sf.jsqlparser.statement.comment.Comment;
import net.sf.jsqlparser.statement.create.index.CreateIndex;
import net.sf.jsqlparser.statement.create.schema.CreateSchema;
import net.sf.jsqlparser.statement.create.sequence.CreateSequence;
import net.sf.jsqlparser.statement.create.synonym.CreateSynonym;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.create.view.AlterView;
import net.sf.jsqlparser.statement.create.view.CreateView;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.drop.Drop;
import net.sf.jsqlparser.statement.execute.Execute;
import net.sf.jsqlparser.statement.grant.Grant;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.merge.Merge;
import net.sf.jsqlparser.statement.merge.MergeInsert;
import net.sf.jsqlparser.statement.merge.MergeUpdate;
import net.sf.jsqlparser.statement.replace.Replace;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.show.ShowTablesStatement;
import net.sf.jsqlparser.statement.truncate.Truncate;
import net.sf.jsqlparser.statement.update.Update;
import net.sf.jsqlparser.statement.upsert.Upsert;
import net.sf.jsqlparser.statement.values.ValuesStatement;

public class SQLStatementVisitor implements StatementVisitor {
    private SQLMetaData sqlMetaData;
    private int level;

    public SQLStatementVisitor(SQLMetaData sqlMetaData, int level) {
        this.sqlMetaData = sqlMetaData;
        this.level = level+1;
    }

    @Override
    public void visit(SavepointStatement savepointStatement) {
        throw new Error("Not Implemented");
    }

    @Override
    public void visit(RollbackStatement rollbackStatement) {
        throw new Error("Not Implemented");
    }

    @Override
    public void visit(Comment comment) {
        throw new Error("Not Implemented");
    }

    @Override
    public void visit(Commit commit) {
        throw new Error("Not Implemented");
    }

    @Override
    public void visit(Delete delete) {
        if(delete.getTable() != null) {
            String tableAlias = null;
            if(delete.getTable().getAlias() != null)
                tableAlias = delete.getTable().getAlias().getName();
            sqlMetaData.getTableMetaData().getTableDetailsList().add(new TableDetails(delete.getTable().getName(),
                    tableAlias));
        }

        sqlMetaData.setSqlType(delete.getClass().getSimpleName());

        delete.getWhere().accept(new SQLExpressionVisitor(sqlMetaData,
                MetaDataConstant.EXPRESSION_VISITOR_DELETE_BINDS, level));

    }

    @Override
    public void visit(Update update) {
        if(update.getTable() != null) {
            String tableAlias = null;
            if(update.getTable().getAlias() != null)
                tableAlias = update.getTable().getAlias().getName();
            sqlMetaData.getTableMetaData().getTableDetailsList().add(new TableDetails(update.getTable().getName(),
                    tableAlias));
        }

        sqlMetaData.setSqlType(update.getClass().getSimpleName());

        if(update.getSelect() != null) {
            update.getSelect().getSelectBody().accept(new SQLSelectVisitor(
                    sqlMetaData, MetaDataConstant.EXPRESSION_VISITOR_UPDATE_SELECT_BINDS, level));
        }
        for(int i =0; i<update.getColumns().size(); i++) {
            String tableName = null;
            if(update.getColumns().get(i).getTable() != null)
                tableName = update.getColumns().get(i).getTable().getName();
            if(update.getSelect() == null) {
                sqlMetaData.getUpdateMetaData().getUpdateItemMetaDataList().add(
                        new InsertItemMetaData(tableName, update.getColumns().get(i).getColumnName()));
                update.getExpressions().get(i).accept(new SQLExpressionVisitor(sqlMetaData,
                        MetaDataConstant.EXPRESSION_VISITOR_UPDATE_BINDS, level));
            }
        }

        if(update.getWhere() != null) {
            update.getWhere().accept(new SQLExpressionVisitor(sqlMetaData, MetaDataConstant.EXPRESSION_VISITOR_WHERE_BINDS, level));
        }

    }

    @Override
    public void visit(Insert insert) {
        sqlMetaData.setSqlType(insert.getClass().getSimpleName());
        if(insert.getColumns() != null) {
            for (int i = 0; i < insert.getColumns().size(); i++) {
                String tableName = null;
                if (insert.getColumns().get(i).getTable() != null)
                    tableName = insert.getColumns().get(i).getTable().getName();
                sqlMetaData.getInsertMetaData().getInsertItemMetaDataList().add(
                        new InsertItemMetaData(tableName, insert.getColumns().get(i).getColumnName()));
            }
        } else if (insert.getSelect() != null) {
            insert.getSelect().getSelectBody().accept(new SQLSelectVisitor(sqlMetaData, MetaDataConstant.INSERT_SELECT_VISITOR,
                    level));
        } else {
            sqlMetaData.getInsertMetaData().setColumnNotSpecified(true);
        }
        if(insert.getItemsList() != null) {
            insert.getItemsList().accept(new SQLItemListVisitor(sqlMetaData,
                    MetaDataConstant.EXPRESSION_VISITOR_INSERT_BINDS, level));
        }
        if(insert.getTable() != null) {
            String tableAlias = null;
            if(insert.getTable().getAlias() != null)
                tableAlias = insert.getTable().getAlias().getName();
            sqlMetaData.getTableMetaData().getTableDetailsList().add(new TableDetails(insert.getTable().getName(),
                    tableAlias));
        }
    }

    @Override
    public void visit(Replace replace) {
        throw new Error("Not Implemented");
    }

    @Override
    public void visit(Drop drop) {
        throw new Error("Not Implemented");
    }

    @Override
    public void visit(Truncate truncate) {
        throw new Error("Not Implemented");
    }

    @Override
    public void visit(CreateIndex createIndex) {
        throw new Error("Not Implemented");
    }

    @Override
    public void visit(CreateSchema createSchema) {
        throw new Error("Not Implemented");
    }

    @Override
    public void visit(CreateTable createTable) {
        throw new Error("Not Implemented");
    }

    @Override
    public void visit(CreateView createView) {
        throw new Error("Not Implemented");
    }

    @Override
    public void visit(AlterView alterView) {
        throw new Error("Not Implemented");
    }

    @Override
    public void visit(Alter alter) {
        throw new Error("Not Implemented");
    }

    @Override
    public void visit(Statements statements) {
        throw new Error("Not Implemented");
    }

    @Override
    public void visit(Execute execute) {
        throw new Error("Not Implemented");
    }

    @Override
    public void visit(SetStatement setStatement) {
        throw new Error("Not Implemented");
    }

    @Override
    public void visit(ResetStatement resetStatement) {
        throw new Error("Not Implemented");
    }

    @Override
    public void visit(ShowColumnsStatement showColumnsStatement) {
        throw new Error("Not Implemented");
    }

    @Override
    public void visit(ShowTablesStatement showTablesStatement) {
        throw new Error("Not Implemented");
    }

    @Override
    public void visit(Merge merge) {
        sqlMetaData.setSqlType(merge.getClass().getSimpleName());
        if(merge.getTable() != null) {
            String tableAlias = null;
            if(merge.getTable().getAlias() != null)
                tableAlias = merge.getTable().getAlias().getName();
            sqlMetaData.getTableMetaData().getTableDetailsList().add(new TableDetails(merge.getTable().getName(),
                    tableAlias));
        }

        if(merge.getUsingSelect() != null) {
            merge.getUsingSelect().getSelectBody().accept(new SQLSelectVisitor(sqlMetaData, level));
        }
        merge.getOnCondition().accept(new SQLExpressionVisitor(sqlMetaData,
                MetaDataConstant.EXPRESSION_VISITOR_WHERE_BINDS,
                level));

        if (merge.getMergeInsert() != null) {
            MergeInsert mergeInsert = merge.getMergeInsert();
            for (int i = 0; i < mergeInsert.getColumns().size(); i++) {
                Column column = mergeInsert.getColumns().get(i);
                String tableName = null;
                if (column.getTable() != null) {
                    tableName = column.getTable().getName();
                }
                InsertItemMetaData insertItemMetaData = new InsertItemMetaData(tableName, column.getColumnName());
                sqlMetaData.getMergeMetaData().getInsertItemMetaDataList().add(insertItemMetaData);
                mergeInsert.getValues().get(i).accept(new SQLExpressionVisitor(sqlMetaData,
                        MetaDataConstant.EXPRESSION_VISITOR_INSERT_BINDS, level, i));
            }
        }

        if (merge.getMergeUpdate() != null) {
            MergeUpdate mergeUpdate = merge.getMergeUpdate();
            for (int i = 0; i < mergeUpdate.getColumns().size(); i++) {
                Column column = mergeUpdate.getColumns().get(i);
                String tableName = null;
                if (column.getTable() != null) {
                    tableName = column.getTable().getName();
                }
                InsertItemMetaData updateItemMetaData = new InsertItemMetaData(tableName, column.getColumnName());
                sqlMetaData.getMergeMetaData().getUpdateItemMetaDataList().add(updateItemMetaData);

                mergeUpdate.getValues().get(i).accept(new SQLExpressionVisitor(sqlMetaData,
                        MetaDataConstant.EXPRESSION_VISITOR_UPDATE_BINDS, level, i));
            }

            if(mergeUpdate.getWhereCondition() != null) {
                mergeUpdate.getWhereCondition().accept(new SQLExpressionVisitor(sqlMetaData,
                        MetaDataConstant.EXPRESSION_VISITOR_WHERE_BINDS, level));
            }

            if(mergeUpdate.getDeleteWhereCondition() != null) {
                mergeUpdate.getDeleteWhereCondition().accept(new SQLExpressionVisitor(sqlMetaData,
                        MetaDataConstant.EXPRESSION_VISITOR_WHERE_BINDS, level));
            }
        }
    }

    @Override
    public void visit(Select select) {
        sqlMetaData.setSqlType(select.getClass().getSimpleName());
        SQLSelectVisitor selectVisitor = new SQLSelectVisitor(sqlMetaData, level);
        select.getSelectBody().accept(selectVisitor);
    }

    @Override
    public void visit(Upsert upsert) {
        throw new Error("Not Implemented");
    }

    @Override
    public void visit(UseStatement useStatement) {
        throw new Error("Not Implemented");
    }

    @Override
    public void visit(Block block) {
        throw new Error("Not Implemented");
    }

    @Override
    public void visit(ValuesStatement valuesStatement) {
        throw new Error("Not Implemented");
    }

    @Override
    public void visit(DescribeStatement describeStatement) {
        throw new Error("Not Implemented");
    }

    @Override
    public void visit(ExplainStatement explainStatement) {
        throw new Error("Not Implemented");
    }

    @Override
    public void visit(ShowStatement showStatement) {
        throw new Error("Not Implemented");
    }

    @Override
    public void visit(DeclareStatement declareStatement) {
        throw new Error("Not Implemented");
    }

    @Override
    public void visit(Grant grant) {
        throw new Error("Not Implemented");
    }

    @Override
    public void visit(CreateSequence createSequence) {
        throw new Error("Not Implemented");
    }

    @Override
    public void visit(AlterSequence alterSequence) {
        throw new Error("Not Implemented");
    }

    @Override
    public void visit(CreateFunctionalStatement createFunctionalStatement) {
        throw new Error("Not Implemented");
    }

    @Override
    public void visit(CreateSynonym createSynonym) {
        throw new Error("Not Implemented");
    }

    @Override
    public void visit(AlterSession alterSession) {
        throw new Error("Not Implemented");
    }

    @Override
    public void visit(IfElseStatement ifElseStatement) {
        throw new Error("Not Implemented");
    }

    @Override
    public void visit(RenameTableStatement renameTableStatement) {
        throw new Error("Not Implemented");
    }

    @Override
    public void visit(PurgeStatement purgeStatement) {
        throw new Error("Not Implemented");
    }

    @Override
    public void visit(AlterSystemStatement alterSystemStatement) {
        throw new Error("Not Implemented");
    }
}

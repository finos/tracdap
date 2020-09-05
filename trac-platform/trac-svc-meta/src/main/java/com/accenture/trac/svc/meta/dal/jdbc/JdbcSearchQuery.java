package com.accenture.trac.svc.meta.dal.jdbc;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

class JdbcSearchQuery {

    private final String query;
    private final List<ParamSetter> params;

    private final List<Fragment> fragments;
    private final int subQueryNumber;
    private final int nextSubQueryNumber;
    private final int nextAttrNumber;

    JdbcSearchQuery(int subQueryNumber, int nextAttrNumber, List<Fragment> fragments) {

        this(subQueryNumber, subQueryNumber + 1, nextAttrNumber, fragments);
    }

    JdbcSearchQuery(int subQueryNumber, int nextSubQueryNumber, int nextAttrNumber, List<Fragment> fragments) {

        this.subQueryNumber = subQueryNumber;
        this.nextSubQueryNumber = nextSubQueryNumber;
        this.nextAttrNumber = nextAttrNumber;
        this.fragments = fragments;

        this.query = "";
        this.params = List.of();
    }

    JdbcSearchQuery(String query, List<ParamSetter> params) {

        this.query = query;
        this.params = params;

        this.subQueryNumber = 0;
        this.nextSubQueryNumber = 1;
        this.nextAttrNumber = 0;
        this.fragments = List.of();
    }

    String getQuery() {
        return query;
    }

    List<ParamSetter> getParams() {
        return params;
    }

    public List<Fragment> getFragments() {
        return fragments;
    }

    public int getSubQueryNumber() {
        return subQueryNumber;
    }

    public int getNextSubQueryNumber() {
        return nextSubQueryNumber;
    }

    public int getNextAttrNumber() {
        return nextAttrNumber;
    }

    static class Fragment {

        private final String joinClause;
        private final String whereClause;
        private final List<ParamSetter> params;

        Fragment(String joinClause, String whereClause, List<ParamSetter> params) {
            this.joinClause = joinClause;
            this.whereClause = whereClause;
            this.params = params;
        }

        String getJoinClause() {
            return joinClause;
        }

        String getWhereClause() {
            return whereClause;
        }

        List<ParamSetter> getParams() {
            return params;
        }
    }

    @FunctionalInterface
    interface ParamSetter {

        void accept(PreparedStatement stmt, int pIndex) throws SQLException;
    }
}

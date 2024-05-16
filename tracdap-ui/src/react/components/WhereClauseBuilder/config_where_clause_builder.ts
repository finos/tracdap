/**
 * Some additional configuration for the WhereClauseBuilder component.
 */

import type {BasicTypeDetails, Option} from "../../../types/types_general";
import {tracdap as trac} from "@finos/tracdap-web-api";

type Configuration = {
    operatorOptions: Option<string, BasicTypeDetails>[]
    booleanOptions: Option<boolean>[]
}

const configuration: Configuration = {

    operatorOptions: [
        {value: "=", label: "equal to", details: {basicTypes: [trac.FLOAT, trac.INTEGER, trac.DECIMAL, trac.DATE, trac.DATETIME, trac.STRING, trac.BOOLEAN]}},
        {value: "!=", label: "not equal to", details: {basicTypes: [trac.FLOAT, trac.INTEGER, trac.DECIMAL, trac.DATE, trac.DATETIME, trac.STRING, trac.BOOLEAN]}},
        {value: ">=", label: ">=", details: {basicTypes: [trac.FLOAT, trac.INTEGER, trac.DECIMAL, trac.DATE, trac.DATETIME]}},
        {value: "<=", label: "<=", details: {basicTypes: [trac.FLOAT, trac.INTEGER, trac.DECIMAL, trac.DATE, trac.DATETIME]}},
        {value: ">", label: ">", details: {basicTypes: [trac.FLOAT, trac.INTEGER, trac.DECIMAL, trac.DATE, trac.DATETIME]}},
        {value: "<", label: "<", details: {basicTypes: [trac.FLOAT, trac.INTEGER, trac.DECIMAL, trac.DATE, trac.DATETIME]}},
        {value: "between", label: "between", details: {basicTypes: [trac.FLOAT, trac.INTEGER, trac.DECIMAL, trac.DATE, trac.DATETIME]}},
        {value: "in", label: "in", details: {basicTypes: [trac.STRING, trac.FLOAT, trac.INTEGER, trac.DECIMAL]}},
        {value: "not in", label: "not in", details: {basicTypes: [trac.STRING, trac.FLOAT, trac.INTEGER, trac.DECIMAL]}},
        {value: "LIKE", label: "like", details: {basicTypes: [trac.STRING]}},
        {value: "is NULL", label: "is null", details: {basicTypes: [trac.FLOAT, trac.INTEGER, trac.DECIMAL, trac.DATE, trac.DATETIME, trac.STRING, trac.BOOLEAN]}},
        {value: "is NOT NULL", label: "is not null", details: {basicTypes: [trac.FLOAT, trac.INTEGER, trac.DECIMAL, trac.DATE, trac.DATETIME, trac.STRING, trac.BOOLEAN]}}
    ],
    booleanOptions: [
        {value: true, label: "True"},
        {value: false, label: "False"},
    ]
}

export {configuration}
/**
 * Some additional configuration for the OverlayBuilder component.
 */

import type {BasicTypeDetails, Option} from "../../../types/types_general";
import {tracdap as trac} from "@finos/tracdap-web-api";

type Configuration = {
    booleanOptions: Option<boolean>[]
    directionOptions: Option<"level" | "percent" | "=", BasicTypeDetails>[]
}

const configuration: Configuration = {

    booleanOptions: [
        {value: true, label: "True"},
        {value: false, label: "False"},
    ],
    directionOptions: [
        {value: "level", label: "Change level by", details: {basicTypes: [trac.FLOAT, trac.INTEGER, trac.DECIMAL]}},
        {value: "percent", label: "Change by a percentage", details: {basicTypes: [trac.FLOAT, trac.INTEGER, trac.DECIMAL]}},
        {value: "=", label: "Equal to", details: {basicTypes: [trac.FLOAT, trac.INTEGER, trac.DECIMAL, trac.DATE, trac.DATETIME, trac.STRING, trac.BOOLEAN]}},
    ]
}

export {configuration}
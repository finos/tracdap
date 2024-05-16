/**
 * A set of types that primarily deal with metadata attributes or model parameters.
 */

import type {DateFormat, DatetimeFormat, GenericGroup, Modify, Option} from "./types_general";
import {tracdap as trac} from "@finos/tracdap-web-api";

export type UiAttributesListRow = {

    "OBJECT_TYPES": null | string
    "ID": null | string
    "NAME": null | string
    "DESCRIPTION": null | string
    "CATEGORY": null | string
    "TOOLTIP": null | string
    "HIDDEN": boolean
    "LINKED_TO_ID": null | string
    "LINKED_TO_VALUE": null | string
    "DEFAULT_VALUE": null | string
    "BASIC_TYPE": null | trac.BasicType
    "FORMAT_CODE": null | string | DateFormat | DatetimeFormat
    "MINIMUM_VALUE": null | string
    "MAXIMUM_VALUE": null | string
    "USE_OPTIONS": boolean
    "OPTIONS_INCREMENT": null | string | DateFormat | DatetimeFormat
    "OPTIONS_VALUES": null | string
    "OPTIONS_LABELS": null | string
    "IS_MULTI": boolean
    "MUST_VALIDATE": boolean
    "RESERVED_FOR_APPLICATION": boolean,
    "NUMBER_OF_ROWS": number,
    "WIDTH_MULTIPLIER": number,
    "SET_AUTOMATICALLY_BY_APPLICATION": boolean
}
export type UiParametersListRow = {

    "OBJECT_TYPES": null | string
    "ID": null | string
    "NAME": null | string
    "DESCRIPTION": null | string
    "CATEGORY": null | string
    "TOOLTIP": null | string
    "HIDDEN": boolean
    "LINKED_TO_ID": null | string
    "LINKED_TO_VALUE": null | string
    "DEFAULT_VALUE": null | string
    "BASIC_TYPE": null | trac.BasicType
    "FORMAT_CODE": null | string | DateFormat | DatetimeFormat
    "MINIMUM_VALUE": null | string
    "MAXIMUM_VALUE": null | string
    "USE_OPTIONS": boolean
    "OPTIONS_INCREMENT": null | string | DateFormat | DatetimeFormat
    "OPTIONS_VALUES": null | string
    "OPTIONS_LABELS": null | string
    "IS_MULTI": boolean
    "MUST_VALIDATE": boolean
    "RESERVED_FOR_APPLICATION": boolean,
    "NUMBER_OF_ROWS": number,
    "WIDTH_MULTIPLIER": number,
    "SET_AUTOMATICALLY_BY_APPLICATION": boolean
}

export type UiAttributesObject1 = {

    "basicType": trac.BasicType
    "category": string
    "defaultValue": UiAttributesListRow["DEFAULT_VALUE"]
    "description": UiAttributesListRow["DESCRIPTION"]
    "disabled": boolean
    "formatCode": null | string | DateFormat | DatetimeFormat
    "hidden": boolean
    "id": string
    "isMulti": boolean
    "linkedToId": UiAttributesListRow["LINKED_TO_ID"]
    "linkedToValue": UiAttributesListRow["LINKED_TO_VALUE"]
    "objectTypes": string[]
    "options": undefined
    "optionsIncrement": UiAttributesListRow["OPTIONS_INCREMENT"]
    "optionLabels": UiAttributesListRow["OPTIONS_LABELS"]
    "optionValues": UiAttributesListRow["OPTIONS_VALUES"]
    "maximumValue": UiAttributesListRow["MAXIMUM_VALUE"]
    "minimumValue": UiAttributesListRow["MINIMUM_VALUE"]
    "mustValidate": boolean
    "name": UiAttributesListRow["NAME"]
    "tooltip": undefined | string
    "useOptions": boolean
    "widthMultiplier": number
    "numberOfRows": number
}

export type UiAttributesObject2 =
    Modify<UiAttributesObject1, { "options": undefined | Option[] | GenericGroup[] }>
    & { "specialType"?: "PASSWORD" | "TEXTAREA" | "OPTION", "textareaRows"?: number }

export type UiAttributesObject3 = Modify<UiAttributesObject2, { minimumValue: null | number | string, maximumValue: null | number | string }>

export type UiAttributesProps = Omit<Modify<UiAttributesObject3, { defaultValue: null | number | string | boolean | Option | Option[] }>, "optionsIncrement" | "optionLabels" | "optionValues">
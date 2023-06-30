import {tracdap as trac} from "@finos/tracdap-web-api";
import {UiParametersListRow} from "../../../../types/types_attributes_and_parameters";

export const uiParametersListAttrs: trac.metadata.ITagUpdate[] = [
    {
        attrName: "key", value: {stringValue: "ui_parameters_list"}
    },
    {
        attrName: "name", value: {stringValue: "User defined model parameter list"}
    },
    {
        attrName: "description",
        value: {
            stringValue: "When a user wants to run a calculation there may be model parameters that need to be set in the user interface. While some of the information about these parameters, such as the label and default value, can be set in the model code greater detail may be needed. This dataset defines additional information about a model's parameter to provide a richer user experience."
        }
    },
    {
        attrName: "show_in_search_results", value: {booleanValue: false}
    }
]

export const uiParametersListSchema: trac.metadata.IFieldSchema[] = [
    {
        fieldName: "OBJECT_TYPES",
        label: "Object types parameters can be added to",
        fieldType: trac.STRING,
        fieldOrder: 0,
        categorical: true,
        businessKey: false
    },
    {
        fieldName: "ID",
        label: "ID",
        fieldType: trac.STRING,
        fieldOrder: 1,
        categorical: true,
        businessKey: false
    },
    {
        fieldName: "NAME",
        label: "Name",
        fieldType: trac.STRING,
        fieldOrder: 2,
        categorical: true,
        businessKey: false
    },
    {
        fieldName: "DESCRIPTION",
        label: "Description",
        fieldType: trac.STRING,
        fieldOrder: 3,
        categorical: false,
        businessKey: false
    },
    {
        fieldName: "CATEGORY",
        label: "Category",
        fieldType: trac.STRING,
        fieldOrder: 4,
        categorical: true,
        businessKey: false
    },
    {
        fieldName: "TOOLTIP",
        label: "Tooltip",
        fieldType: trac.STRING,
        fieldOrder: 5,
        categorical: false,
        businessKey: false
    },
    {
        fieldName: "HIDDEN",
        label: "Hidden",
        fieldType: trac.BOOLEAN,
        fieldOrder: 6,
        categorical: false,
        businessKey: false
    },
    {
        fieldName: "LINKED_TO_ID",
        label: "Show this parameter based on another parameter's value",
        fieldType: trac.STRING,
        fieldOrder: 7,
        categorical: true,
        businessKey: false
    },
    {
        fieldName: "LINKED_TO_VALUE",
        label: "Linked parameter's value required to show this parameter",
        fieldType: trac.STRING,
        fieldOrder: 8,
        categorical: false,
        businessKey: false
    },
    {
        fieldName: "BASIC_TYPE",
        label: "Parameter type",
        fieldType: trac.INTEGER,
        fieldOrder: 9,
        categorical: false,
        businessKey: false
    },
    {
        fieldName: "DEFAULT_VALUE",
        label: "Default value",
        fieldType: trac.STRING,
        fieldOrder: 10,
        categorical: false,
        businessKey: false
    },
    {
        fieldName: "FORMAT_CODE",
        label: "Parameter format code",
        fieldType: trac.STRING,
        fieldOrder: 11,
        categorical: true,
        businessKey: false
    },
    {
        fieldName: "MINIMUM_VALUE",
        label: "Minimum value or length",
        fieldType: trac.STRING,
        fieldOrder: 12,
        categorical: false,
        businessKey: false
    },
    {
        fieldName: "MAXIMUM_VALUE",
        label: "Maximum value or length",
        fieldType: trac.STRING,
        fieldOrder: 13,
        categorical: false,
        businessKey: false
    },
    {
        fieldName: "USE_OPTIONS",
        label: "User must select from a range of options",
        fieldType: trac.BOOLEAN,
        fieldOrder: 14,
        categorical: false,
        businessKey: false
    },
    {
        fieldName: "OPTIONS_INCREMENT",
        label: "Increment when shown as an option",
        fieldType: trac.STRING,
        fieldOrder: 15,
        categorical: true,
        businessKey: false
    },
    {
        fieldName: "OPTIONS_VALUES",
        label: "Option values",
        fieldType: trac.STRING,
        fieldOrder: 16,
        categorical: false,
        businessKey: false
    },
    {
        fieldName: "OPTIONS_LABELS",
        label: "Option labels",
        fieldType: trac.STRING,
        fieldOrder: 17,
        categorical: false,
        businessKey: false
    },
    {
        fieldName: "IS_MULTI",
        label: "Allow multiple options to be selected",
        fieldType: trac.BOOLEAN,
        fieldOrder: 18,
        categorical: false,
        businessKey: false
    },
    {
        fieldName: "MUST_VALIDATE",
        label: "User set values must validate before saving",
        fieldType: trac.BOOLEAN,
        fieldOrder: 19,
        categorical: false,
        businessKey: false
    },
    {
        fieldName: "RESERVED_FOR_APPLICATION",
        label: "Whether the user can edit the parameter",
        fieldType: trac.BOOLEAN,
        fieldOrder: 20,
        categorical: false,
        businessKey: false
    },
    {
        fieldName: "NUMBER_OF_ROWS",
        label: "Number of rows for text areas",
        fieldType: trac.INTEGER,
        fieldOrder: 21,
        categorical: false,
        businessKey: false
    },
    {
        fieldName: "WIDTH_MULTIPLIER",
        label: "Increase the width of the parameter in the menu",
        fieldType: trac.INTEGER,
        fieldOrder: 22,
        categorical: false,
        businessKey: false
    },
    {
        fieldName: "SET_AUTOMATICALLY_BY_APPLICATION",
        label: "Is the parameter set by the application rather than by the user",
        fieldType: trac.BOOLEAN,
        fieldOrder: 23,
        categorical: false,
        businessKey: false
    }
]

export const defaultUiParametersList: UiParametersListRow[] = [
    {
        "OBJECT_TYPES": "ALL",
        "ID": "advanced_logging",
        "NAME": "Advanced logging",
        "DESCRIPTION": "Turning this on will add additional information about the calculation to the logs.",
        "CATEGORY": "DEBUG",
        "TOOLTIP": null,
        "HIDDEN": false,
        "LINKED_TO_ID": null,
        "LINKED_TO_VALUE": null,
        "DEFAULT_VALUE": "TRUE",
        "BASIC_TYPE": trac.BOOLEAN,
        "FORMAT_CODE": null,
        "MINIMUM_VALUE": null,
        "MAXIMUM_VALUE": null,
        "USE_OPTIONS": false,
        "OPTIONS_INCREMENT": null,
        "OPTIONS_VALUES": null,
        "OPTIONS_LABELS": null,
        "IS_MULTI": false,
        "MUST_VALIDATE": false,
        "RESERVED_FOR_APPLICATION": true,
        "NUMBER_OF_ROWS": 1,
        "WIDTH_MULTIPLIER": 1,
        "SET_AUTOMATICALLY_BY_APPLICATION": false
    }
]
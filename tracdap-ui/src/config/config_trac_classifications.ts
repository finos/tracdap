import {JobStatusDetails, ObjectTypesString, Option, OptionDetailsSchema} from "../types/types_general"
import {tracdap as trac} from "@finos/tracdap-web-api";

/**
 * A set of options for the SelectOption component that allow the user to select from a range of TRAC API types. For
 * example when searching TRAC the user might want to limit their search to MODELS only.
 */
const Types: {

    tracBasicTypes: Option<string, trac.BasicType>[]
    tracAdvancedTypes: Option<string, trac.BasicType>[]
    tracObjectTypes: Option<ObjectTypesString, trac.ObjectType>[]
    tracJobStatuses: Option<string, JobStatusDetails>[]
    tracJobTypes: Option<string>[]
    tracFieldItems: Option<string, OptionDetailsSchema>[]

} = {

    tracBasicTypes: [
        {value: "STRING", label: "String", type: trac.STRING},
        {value: "INTEGER", label: "Integer", type: trac.INTEGER},
        {value: "FLOAT", label: "Float", type: trac.FLOAT},
        // {value: "DECIMAL", label: "Decimal", type: trac.DECIMAL},
        {value: "BOOLEAN", label: "Boolean", type: trac.BOOLEAN},
        {value: "DATE", label: "Date", type: trac.DATE},
        {value: "DATETIME", label: "Datetime", type: trac.DATETIME}
    ],
    tracAdvancedTypes: [
        {value: "MAP", label: "Object", type: trac.BasicType.MAP},
        {value: "ARRAY", label: "Array", type: trac.BasicType.ARRAY},
    ],
    tracObjectTypes: [
        {value: "DATA", label: "Data", type: trac.ObjectType.DATA},
        {value: "MODEL", label: "Model", type: trac.ObjectType.MODEL},
        {value: "FLOW", label: "Flow", type: trac.ObjectType.FLOW},
        {value: "JOB", label: "Job", type: trac.ObjectType.JOB},
        {value: "FILE", label: "File", type: trac.ObjectType.FILE},
        {value: "SCHEMA", label: "Schema", type: trac.ObjectType.SCHEMA},
        {value: "CUSTOM", label: "Custom", type: trac.ObjectType.CUSTOM},
        {value: "STORAGE", label: "Storage", type: trac.ObjectType.STORAGE}
    ],
    tracJobStatuses: [
        {value: "ALL", label: "All", disabled: false,  details: {variant: "info", message: "Do not use"}},
        {value: "PREPARING", label: "Preparing", disabled: false, details: {variant: "warning", message: "This job is currently being prepared for execution"}},
        {value: "VALIDATED", label: "Validated", disabled: false, details: {variant: "light", message: "This job is currently being validated before submission"}},
        {value: "PENDING", label: "Pending", disabled: false, details: {variant: "light", message: "This job is currently pending submission"}},
        {value: "QUEUED", label: "Queued", disabled: false, details: {variant: "light", message: "This job is currently queued for execution"}},
        {value: "SUBMITTED", label: "Submitted", disabled: false, details: {variant: "warning", message: "This job is currently submitted for execution"}},
        {value: "RUNNING", label: "Running", disabled: false, details: {variant: "warning", message: "This job is currently running"}},
        {value: "FINISHING", label: "Finishing", disabled: false, details: {variant: "warning", message: "This job is currently finishing before completion"}},
        {value: "SUCCEEDED", label: "Succeeded", disabled: false, details: {variant: "success", message: "This job ran successfully"}},
        {value: "FAILED", label: "Failed", disabled: false, details: {variant: "danger", message: "This job failed"}},
        {value: "CANCELLED", label: "Cancelled", disabled: false, details: {variant: "danger", message: "This job was cancelled"}},
        {value: "JOB_STATUS_CODE_NOT_SET", label: "Status not set", disabled: false, details: {variant: "danger", message: "The status of this job is unknown"}},
    ],
    tracJobTypes: [
        {value: "ALL", label: "All", disabled: false},
        {value: "RUN_MODEL", label: "Run model", disabled: false},
        {value: "RUN_FLOW", label: "Run flow", disabled: false},
        {value: "IMPORT_MODEL", label: "Import model", disabled: false},
        {value: "IMPORT_DATA", label: "Import data", disabled: false}
    ],
    // The fields that make up a schema
    tracFieldItems: [
        {
            value: "FIELD_ORDER",
            label: "Field order",
            details: {optional: true, jsonName: "fieldOrder", listName: "field_order"}
        },
        {
            value: "FIELD_NAME",
            label: "Field name",
            details: {optional: false, jsonName: "fieldName", listName: "field_name"}
        },
        {
            value: "FIELD_TYPE",
            label: "Field type",
            details: {optional: false, jsonName: "fieldType", listName: "field_type"}
        },
        {
            value: "LABEL",
            label: "Field label",
            details: {optional: true, jsonName: "fieldLabel", listName: "field_label"}
        },
        {
            value: "FORMAT_CODE",
            label: "Format code",
            details: {optional: true, jsonName: "formatCode", listName: "format_code"}
        },
        {
            value: "CATEGORICAL",
            label: "Categorical",
            details: {optional: true, jsonName: "categorical", listName: "categorical"}
        },
        {
            value: "BUSINESS_KEY",
            label: "Business key",
            details: {optional: true, jsonName: "business_key", listName: "business_key"}
        }
    ]
}

export {Types}
import {tracdap as trac} from "@finos/tracdap-web-api";
import {UiBatchImportDataRow} from "../../../../types/types_general";

export const uiBatchImportDataAttrs: trac.metadata.ITagUpdate[] = [
    {
        attrName: "key",
        value: {stringValue: "ui_batch_import_data"}
    },
    {
        attrName: "name",
        value: {stringValue: "Batch data imports"}
    },
    {
        attrName: "description",
        value: {
            stringValue: "In TRAC data can be brought into the platform by uploading local Excel or CSV files or by batch import from other platforms. This dataset enables batch imports to be configured so they can be executed."
        }
    },
    {
        attrName: "show_in_search_results",
        value: {booleanValue: false}
    }
]

export const uiBatchImportDataSchema: trac.metadata.IFieldSchema[] = [

    {fieldName: "BUSINESS_SEGMENTS", fieldOrder: 0, label: "Business segments", fieldType: trac.STRING, categorical: true, businessKey: false},
    {fieldName: "DATASET_ID", fieldOrder: 1, label: "Dataset ID", fieldType: trac.STRING, categorical: true, businessKey: false},
    {fieldName: "DATASET_NAME", fieldOrder: 2, label: "Dataset name", fieldType: trac.STRING, categorical: false, businessKey: false},
    {fieldName: "DATASET_DESCRIPTION", fieldOrder: 3, label: "Dataset description", fieldType: trac.STRING, categorical: false, businessKey: false},
    {fieldName: "DATASET_FREQUENCY", fieldOrder: 4, label: "Frequency of batch import", fieldType: trac.STRING, categorical: true, businessKey: false},
    {fieldName: "DATASET_SOURCE_SYSTEM", fieldOrder: 5, label: "Source system", fieldType: trac.STRING, categorical: true, businessKey: false},
    {fieldName: "DATASET_DATE_REGEX", fieldOrder: 6, label: "Regex to find date in filename", fieldType: trac.STRING, categorical: false, businessKey: false},
    {fieldName: "RECONCILIATION_FIELDS", fieldOrder: 7, label: "Reconciliation fields", fieldType: trac.STRING, categorical: false, businessKey: false},
    {fieldName: "RECONCILIATION_ITEM_SUFFIX", fieldOrder: 8, label: "Reconciliation file suffix", fieldType: trac.STRING, categorical: false, businessKey: false},
    {fieldName: "RECONCILIATION_ITEM_FORMAT", fieldOrder: 9, label: "Reconciliation file format", fieldType: trac.STRING, categorical: false, businessKey: false},
    {fieldName: "RECONCILIATION_ITEM_FORMAT_MODIFIER", fieldOrder: 10, label: "Reconciliation file format modifier", fieldType: trac.STRING, categorical: false, businessKey: false},
    {fieldName: "NEW_FILES_ONLY", fieldOrder: 11, label: "New files only", fieldType: trac.BOOLEAN, categorical: false, businessKey: false},
    {fieldName: "DISABLED", fieldOrder: 12, label: "Disabled import", fieldType: trac.BOOLEAN, categorical: false, businessKey: false}
]

export const defaultUiBatchImportData: UiBatchImportDataRow[] = []
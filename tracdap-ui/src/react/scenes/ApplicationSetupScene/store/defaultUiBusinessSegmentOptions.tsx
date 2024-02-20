import {tracdap as trac} from "@finos/tracdap-web-api";
import {UiBusinessSegmentsDataRow} from "../../../../types/types_general";

export const uiBusinessSegmentOptionsAttrs: trac.metadata.ITagUpdate[] = [
    {
        attrName: "key", value: {stringValue: "ui_business_segment_options"}
    },
    {
        attrName: "name", value: {stringValue: "Business segment options"}
    },
    {
        attrName: "description",
        value: {
            stringValue: "When an object such as a dataset, model or file is added into TRAC, metadata is attached to the item to help users understand what it is. One example is the business segment(s) that should be associated with the item. For example whether a model applies to a particular portfolio or asset type or geography. This dataset defines the business segments that can be attached to objects in TRAC."
        }
    },
    {
        attrName: "show_in_search_results", value: {booleanValue: false}
    }
]

export const uiBusinessSegmentOptionsSchema: trac.metadata.IFieldSchema[] = [
    {
        fieldName: "GROUP_01_ID",
        label: "Business segment group 1 ID",
        fieldType: trac.STRING,
        fieldOrder: 0,
        categorical: true,
        businessKey: false
    },
    {
        fieldName: "GROUP_01_NAME",
        label: "Business segment group 1",
        fieldType: trac.STRING,
        fieldOrder: 1,
        categorical: true,
        businessKey: false
    },
    {
        fieldName: "GROUP_02_ID",
        label: "Business segment group 2 ID",
        fieldType: trac.STRING,
        fieldOrder: 2,
        categorical: true,
        businessKey: false
    },
    {
        fieldName: "GROUP_02_NAME",
        label: "Business segment group 2",
        fieldType: trac.STRING,
        fieldOrder: 3,
        categorical: true,
        businessKey: false
    },
    {
        fieldName: "GROUP_03_ID",
        label: "Business segment group 3 ID",
        fieldType: trac.STRING,
        fieldOrder: 4,
        categorical: true,
        businessKey: false
    },
    {
        fieldName: "GROUP_03_NAME",
        label: "Business segment group 3",
        fieldType: trac.STRING,
        fieldOrder: 5,
        categorical: true,
        businessKey: false
    },
    {
        fieldName: "GROUP_04_ID",
        label: "Business segment group ID",
        fieldType: trac.STRING,
        fieldOrder: 6,
        categorical: true,
        businessKey: false
    },
    {
        fieldName: "GROUP_04_NAME",
        label: "Business segment group 4 name",
        fieldType: trac.STRING,
        fieldOrder: 7,
        categorical: true,
        businessKey: false
    }
]

export const defaultUiBusinessSegmentOptions: UiBusinessSegmentsDataRow[] = []
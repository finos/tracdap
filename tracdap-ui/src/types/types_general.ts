import {ActionCreatorWithPayload, AsyncThunk} from "@reduxjs/toolkit";
import {ElkNode} from "elkjs/lib/elk.bundled";
import {ElkExtendedEdge, ElkPort} from "elkjs/lib/elk-api";
import {FilterOptionOption} from "react-select/dist/declarations/src/filters";
import {InitialTableState} from "@tanstack/react-table";
import {MultiValue, OptionsOrGroups, SingleValue} from "react-select";
import React from "react";
import {tracdap as trac} from "@finos/tracdap-web-api";
import {UiAttributesListRow, UiParametersListRow} from "./types_attributes_and_parameters";
import {Endpoints} from "@octokit/types";
import {GroupBase} from "react-select/dist/declarations/src/types";
import {RootState} from "../storeController";
//import {FullChartState, UseWorkerToGetSegmentOptionsPayload, UseWorkerToLoadDataPayload} from "../react/components/Chart/Highcharts/highcharts_utils_trac_type_checkers";

/**
 * The status of async calls.
 */
export type StoreStatus = "idle" | "pending" | "succeeded" | "failed";

/**
 * The names of cookies created by TRAC-UI.
 */
export type TracUiCookieNames = "trac-tenant" | "trac-theme" | "trac-language";

/**
 * The names of cookies provided by TRAC.
 */
export type TracCookieNames = "trac_user_id" | "trac_user_name" | "trac_session_expiry_utc";

/**
 * The TRAC object types as strings e.g. "DATA" | "MODEL" | "SCHEMA" ...
 */
export type ObjectTypesString = keyof typeof trac.ObjectType

/**
 * The TRAC part types as strings e.g. "PART_BY_RANGE" | "PART_BY_VALUE" ...
 */
export type PartTypesString = keyof typeof trac.PartType

/**
 * The TRAC part types as strings e.g. "PART_ROOT" | "PART_BY_RANGE" | "PART_BY_VALUE" ...
 */
// export type PartTypesString = keyof typeof trac.metadata.PartType

/**
 * The TRAC basic types as strings e.g. "BOOLEAN" | "DATE" | "DATETIME" | "DECIMAL"| "FLOAT" | "INTEGER" |"STRING" ...
 */
export type BasicTypesString = keyof typeof trac.BasicType

/**
 * The TRAC basic stypes that can have formats applied to them.
 */
export type TracBasicTypesWithFormats =
    trac.BasicType.DATE
    | trac.BasicType.DATETIME
    | trac.BasicType.INTEGER
    | trac.BasicType.FLOAT
    | trac.BasicType.DECIMAL

/**
 * The TRAC basic stypes that are for numbers.
 */
export type TracNumberBasicTypes = trac.BasicType.INTEGER | trac.BasicType.FLOAT | trac.BasicType.DECIMAL

/**
 * The TRAC basic stypes that are for dates.
 */
export type TracDateBasicTypes = trac.BasicType.DATE | trac.BasicType.DATETIME

/**
 * The interface for an object returned when getting a dataset from TRAC, the {@link getSmallDatasetByTag}
 * functions return payloads with this interface.
 */
export type GetDatasetByTagResult<key = void, data = void> = {
    tag: trac.metadata.Tag,
    data: (data extends void ? DataRow : data)[],
    schema: trac.metadata.IFieldSchema[]
    key?: key
}

/**
 * Allowed values in TRAC datasets.
 */
export type DataValues = null | boolean | string | number

/**
 * Generic row for a TRAC dataset.
 */
export type DataRow = Record<string, DataValues>

/**
 * Allowed values for a table in the UI, string arrays are added as the FindInTrac component shows a table of
 * search results which includes business segments which are stored as an attribute consisting of an array of strings.
 */
export type TableValues = Date | null | boolean | string | number | string[]

/**
 * Generic row in a for a table in the UI.
 */
export type TableRow = Record<string, TableValues>

/**
 * Allowed formats for numbers, this is stored as a string in a dataset schema but when split into
 * an array this is the format that it should be in.
 */
export type NumberFormatAsArray = [string, string, number | null, string, string, number]

/**
 * Allowed formats for dates.
 */
export type DateFormat = "ISO" | "DAY" | "WEEK" | "MONTH" | "QUARTER" | "HALF_YEAR" | "YEAR"

/**
 * Allowed formats for date times.
 */
export type DatetimeFormat = "DATETIME" | "FILENAME" | "TIME"

/**
 * Allowed formats for all basic types in a TRAC dataset.
 */
export type AllTypeFormats = NumberFormatAsArray | DateFormat | DatetimeFormat

/**
 * A type that converts a TRAC basic type to its equivalent Typescript type.
 */
// TODO this was added early on and then forgotten about, it could be used more widely.
export type BasicTypeConvertor<P extends trac.metadata.BasicType>
    = P extends trac.BasicType.STRING ? string : P extends trac.BasicType.DATE ? string : P extends trac.BasicType.DATETIME ? string : P extends trac.BasicType.FLOAT ? number : P extends trac.BasicType.INTEGER ? number : P extends trac.BasicType.DECIMAL ? number : P extends trac.BasicType.BOOLEAN ? boolean : never

/**
 * A type that allows the type of a property in an object to be changed. This removes the existing definition from an interface
 * and then adds in the new definition.
 */
export type Modify<T, R> = Omit<T, keyof R> & R;

/**
 * The row definition for the business segments dataset that the UI uses to allow the user to set and view
 * business segment attributes.
 */
export type UiBusinessSegmentsDataRow = {

    "GROUP_01_ID": null | string
    "GROUP_01_NAME": null | string
    "GROUP_02_ID": null | string
    "GROUP_02_NAME": null | string
    "GROUP_03_ID": null | string
    "GROUP_03_NAME": null | string
    "GROUP_04_ID": null | string
    "GROUP_04_NAME": null | string
}

/**
 * An interface for the batch import dataset, this dataset is created and edited in the {@link ApplicationSetupScene}.
 */
export type UiBatchImportDataRow = {

    "BUSINESS_SEGMENTS": null | string,
    "DATASET_ID": null | string,
    "DATASET_NAME": null | string,
    "DATASET_DESCRIPTION": null | string,
    "DATASET_FREQUENCY": null | string,
    "DATASET_SOURCE_SYSTEM": null | string,
    "DATASET_DATE_REGEX": null | string,
    "RECONCILIATION_FIELDS": null | string,
    "RECONCILIATION_ITEM_SUFFIX": null | string,
    "RECONCILIATION_ITEM_FORMAT": null | string,
    "RECONCILIATION_ITEM_FORMAT_MODIFIER": null | string,
    "NEW_FILES_ONLY": boolean,
    "DISABLED": boolean
}

/**
 * The keys of datasets created by the {@link ApplicationSetupScene}.
 */
export type UiEditableDatasetKeys = "ui_parameters_list" | "ui_batch_import_data" | "ui_attributes_list" | "ui_business_segment_options"

/**
 * The composite schema of all the datasets created by the {@link ApplicationSetupScene}.
 */
export type UiEditableRow = UiAttributesListRow | UiBatchImportDataRow | UiBusinessSegmentsDataRow | UiParametersListRow

/**
 * The bootstrap variants used by the UI.
 */
export type Variants =
    | 'primary'
    | 'secondary'
    | 'success'
    | 'danger'
    | 'warning'
    | 'info'
    | 'dark'
    | 'light'
    | 'link'
    | 'outline-primary'
    | 'outline-secondary'
    | 'outline-success'
    | 'outline-danger'
    | 'outline-warning'
    | 'outline-info'
    | 'outline-dark'
    | 'outline-light';

/**
 * The type for the menu items in the config that list the items in the SideMenu component.
 */
export type MenuItem = {
    path: string | string[],
    title: string,
    icon: string | null,
    description: string | null,
    showDefaultHeader: boolean,
    hiddenInSideMenu: boolean,
    hiddenInHomeMenu: boolean,
    expandableMenu: boolean,
    openOnLoad?: boolean,
    children: MenuItem[],
    element: React.ReactElement<any, any> | undefined
}

/**
 * The type for the menu items in the config that list the items in the SideMenu component. This is a
 * version where the path arrays have been unwound to individual items for use in the react-router
 * plugin 'useRoutes' hook. This is in the {@link Scenes} component.
 */
export type MappedMenuItem = {
    path: string,
    title: string,
    icon: string | null,
    description: string | null,
    showDefaultHeader: boolean,
    hiddenInSideMenu: boolean,
    hiddenInHomeMenu: boolean,
    expandableMenu: boolean,
    openOnLoad?: boolean,
    children: MappedMenuItem[],
    element: React.ReactElement<any, any> | undefined
}

/**
 * The image details in the client-config.json for client and application images. This is stored in the
 * ApplicationSetup store.
 */
export type ConfigImage = {
    src: string,
    naturalWidth: number
    naturalHeight: number
    displayWidth: number
    displayHeight: number
    alt: string
    style?: React.CSSProperties
}

/**
 * A list of tools that can be used in the links in the footer.
 */
export type ManagementTools = "gitlab" | "github" | "nexus" | "gitkraken" | "bitbucket" | "confluence" | "jira"

/**
 * A list of the theme names available.
 */
export type ThemesList = "lightTheme" | "darkTheme" | "clientTheme"

export type ThemeColours =
    "--secondary-background"
    | "--tertiary-text"
    | "--info"
    | "--quaternary-background"
    | "--info-dark"
    | "--select-toggle-shadow"
    | "--select-toggle-off-color"
    | "--primary-background"
    | "--danger"
    | "--primary-text"
    | "--secondary-text"
    | "--success"
    | "--success-dark"
    | "--tertiary-background"

/**
 * A type for common props between the various select components e.g. SelectOption or SelectValue. Some of these props are returned
 * as part of the payload when the select value changes.
 */
export type SelectCommonProps = {

    /**
     * The css class to apply to the select, this allows additional styles to be added to the component.
     */
    className?: string
    /**
     * Helper text to show the user
     */
    helperText?: string
    /**
     * An identifier for the select that is sent back when the user changes the value. name, id
     * and index together allow for three keys to be attached to each event.
     */
    id?: null | number | string
    /**
     * An identifier for the select that is sent back when the user changes the value. name, id
     * and index together allow for three keys to be attached to each event. Index would usually be
     * used when referencing the position in an array or example.
     */
    index?: number
    /**
     * Whether the select is disabled.
     */
    isDisabled?: boolean
    /**
     * Whether an option must be selected by the user.
     */
    mustValidate?: boolean
    /**
     * An identifier for the select that is sent back when the user changes the value. name, id
     * and index together allow for two keys to be attached to each event.
     */
    name?: null | string
    /**
     * The text to show when no options are selected, this is an override to the
     * default text.
     */
    placeHolderText?: string
    /**
     * Whether to include the space for the validation message in the layout. When showing a mixture of
     * selects, some which need validation and some that don't then this should be true so that the layout
     * is even. However, if you don't need validation, and you want to remove the space reserved for the
     * message then set this to false.
     */
    showValidationMessage?: boolean
    /**
     * The size of the input, this relates to the Bootstrap size.
     */
    size?: SizeList,
    /**
     * An identifier for the select that is sent back when the user changes the value. This is specifically used
     * where a particular section of a tore needs to be updated.
     */
    storeKey?: string
    /**
     * A tool tip to display with the label.
     */
    tooltip?: string
    /**
     * Whether the component should update the store with the validation information when it mounts, this
     * guarantees that even without the user changing the component the store will know if the default value
     * is valid.
     */
    validateOnMount?: boolean
    /**
     * Whether the value of the component has been checked for validation. For example when mounted
     * the value of this should be set to false and no validation messages will show. Then when the
     * user tries to use the value the checkValidity function will run and the value of
     * validationChecked will be updated in the store. If the option is invalid then the validation
     * messages will show.
     */
    validationChecked?: boolean
    /**
     * When showing a label this sets whether to show the label in a single row or a stacked column
     */
    labelPosition?: "top" | "left"
    /**
     *
     * A label to show with the select.
     */
    labelText?: string
}

/**
 * A type interface for the SelectOption component props.
 */
export type SelectOptionProps = SelectCommonProps & {

    /**
     * A function that runs when the user changes the value.
     */
    onChange: Function
        /**
     * Whether the action needs to be dispatched or not. Thunks
     * need to be dispatched.
     */
    // TODO deprecate?
    isDispatched?: boolean
    /**
     * The TRAC basic type of the value returned by the select e.g. 4 which equates to STRING.
     */
    basicType: trac.BasicType
    /**
     * A function that tests that the option value is valid and returns the message to show if not. A standard
     * function is set but by providing this prop the validation can be changed to whatever is needed.
     */
    checkValidity?: (payload: SelectOptionCheckValidityArgs) => CheckValidityReturn
    /**
     * A function that runs when the user changes the value. This is the prop to set when the function needs to
     * be dispatched to a Redux store.
     */
    dispatchedOnChange?: ActionCreatorWithPayload<SelectOptionPayload<Option, IsMulti>>
    /**
     * A function that runs when the user interacts with the select and that filters
     * the options that are shown.
     */
    filterOption?: (option: FilterOptionOption<GenericOption>, inputValue: string, hideDisabledOptions: boolean) => boolean
    /**
     * Whether the disabled options should be hidden.
     */
    hideDisabledOptions?: boolean
    /**
     * Whether the select dropdown menu is hidden, this is useful when you want the select to look like a regular input, for example when adding in options.
     */
    hideDropdown?: boolean
    /**
     * Whether the user can clear their selections rather than just change them.
     */
    isClearable?: boolean
    /**
     * Whether the user can add their own bespoke options to the list.
     */
    isCreatable?: boolean
    /**
     * Whether the select is loading something in the background and should therefore be disabled, this also shows a loading icon.
     */
    isLoading?: boolean
    /**
     * Whether to allow multiple options to be selected.
     */
    isMulti?: boolean
    /**
     * A function that is passed as a prop to the react-select component that sets
     * how to identify disabled options.
     */
    isOptionDisabled?: (option: GenericOption) => boolean
    /**
     * The number of selected options that can be shown before the select shows a summary message instead.
     */
    maximumSelectionsBeforeMessageOverride?: number
    /**
     * The TRAC object type that the select is handling, e.g. 3 which equated to FLOW. This is useful
     * if there is some logic downstream that needs to know what object type is set. For example when
     * allowing users to add their own option to a select we restrict it to only the type loaded into
     * the options by default.
     */
    objectType?: trac.ObjectType
    /**
     * A function that runs when the user creates a new option.
     */
    onCreateNewOption?: ActionCreatorWithPayload<OnCreateNewOptionPayload<Tag | CreateDetails>> |
        AsyncThunk<{ id: string, option: SearchOption }, OnCreateNewOptionPayload<Tag | CreateDetails>, { state: RootState }>
    /**
     * An array of options to show in the select.
     */
    options?: GenericOption[] | GenericGroup[]
    /**
     * The selected options.
     */
    value?: SingleValue<GenericOption> | MultiValue<GenericOption>
    /**
     * There are some instances where we need to attach the menu that opens when the user goes to select an option to a separate div in the UI,
     * this is usually when the SelectOption component is shown inside a table or some such element that would otherwise clip the menu. This prop
     * is used to place the menu in a div with the ID 'custom-react-select'. This div is created in App.tsx.
     */
    useMenuPortalTarget?: boolean
}

/**
 * A type interface for the SelectToggle component props. There is no basic type prop as it only handles BOOLEANS.
 */
export type SelectToggleProps = SelectCommonProps & {
/**
     * A function that runs when the user changes the value.
     */
    onChange: Function
        /**
     * Whether the action needs to be dispatched or not. Thunks
     * need to be dispatched.
     */
    // TODO deprecate?
    isDispatched?: boolean
    /**
     * The toggle value.
     */
    value: boolean | null
    /**
     * A function that tests that the boolean value is valid and returns the message to show if not. A standard
     * function is set but by providing this prop the validation can be changed to whatever is needed.
     */
    checkValidity?: (payload: SelectToggleCheckValidityArgs) => CheckValidityReturn
}

/**
 * A type interface for the SelectDate component props.
 */
export type SelectDateProps = SelectCommonProps & {
/**
     * A function that runs when the user changes the value. This function is not dispatched.
     */
    //onChange?: (payload : SelectDatePayload) => void
    onChange: Function
    isDispatched?: boolean
        /**
     * Whether the action needs to be dispatched or not. Thunks
     * need to be dispatched.
     */
    // TODO deprecate?
    //isDispatched?: boolean
    /**
     * A function that runs when the user changes the value. This is the prop to set when the function needs to
     * be dispatched to a Redux store.
     */
    dispatchedOnChange?: ActionCreatorWithPayload<SelectDatePayload> | AsyncThunk<any, SelectDatePayload, { state: RootState }>
    /**
     * The TRAC basic type of the value returned by the select e.g. 4 which equates to STRING.
     */
    basicType: trac.BasicType.DATE | trac.BasicType.DATETIME
    /**
     * A function that tests that the user option is valid and returns the message to show if not. A standard
     * function is set but by providing this prop the validation can be changed to whatever is needed.
     */
    checkValidity?: (payload: SelectDateCheckValidityArgs) => CheckValidityReturn;
    /**
     * The date format to apply to the date value e.g. "DAY".
     */
    formatCode: DateFormat | DatetimeFormat | null
    /**
     * Whether the user can clear their selection rather than just change them.
     */
    isClearable?: boolean
    /**
     * The last or latest date that can be selected, this is a ISO format string.
     */
    maximumValue?: string | null
    /**
     * The first or earliest date that can be selected, this is a ISO format string.
     */
    minimumValue?: string | null
    /**
     * Whether date values should be set as the start of end of their relevant period, for example when setting a date range by day you want the start date to be at the
     * beginning of the first day (00:00:00) and the end date to be at the very end of the day (23:59:59).
     */
    position?: "start" | "end"
    /**
     * There are some instances where we need to attach the menu that opens when the user goes to select an option to a separate div in the UI,
     * this is usually when the SelectOption component is shown inside a table or some such element that would otherwise clip the menu. This prop
     * is used to place the menu in a div with the ID 'datepicker-root-portal'. This div is created in by the 'react-datepicker' package.
     */
    useMenuPortalTarget?: boolean
    /**
     * The date value as a string.
     */
    value: string | null
}

export type SelectDateRangeProps =
    Pick<SelectCommonProps, "storeKey" | "id" | "name" | "isDisabled" | "className" | "tooltip" | "labelText" | "labelPosition">
    & {
    startDate: string
    endDate: string
    setDayOrMonth: Function
    setCreateOrUpdate: Function
    dayOrMonth: DateFormat | DatetimeFormat
    createOrUpdate: "CREATE" | "UPDATE"
    /**
     * A function that runs when the user changes the value.
     */
    onChange: Function
        /**
     * Whether the action needs to be dispatched or not. Thunks
     * need to be dispatched.
     */
    // TODO deprecate?
    isDispatched?: boolean
}

export type SelectValueExtraProps = {

    /**
     * A function that runs when the user changes the value.
     */
    onChange: Function
        /**
     * Whether the action needs to be dispatched or not. Thunks
     * need to be dispatched.
     */
    // TODO deprecate?
    isDispatched?: boolean
    /**
     * A function that runs when the user changes the value. This is the prop to set when the function needs to
     * be dispatched to a Redux store.
     */
    dispatchedOnChange?: ActionCreatorWithPayload<SelectValuePayload>
    /**
     * The maximum value to pass validation if it is a number or the maximum length if it is a string.
     */
    maximumValue?: number | null
    /**
     * The minimum value to pass validation if it is a number or the minimum length if it is a string.
     */
    minimumValue?: number | null
    /**
     * Whether the input is read only and not editable. It will not be greyed out.
     */
    readOnly?: boolean
    /**
     * A function that tests that the user value is valid and returns the message to show if not. A standard
     * function is set but by providing this prop the validation can be changed to whatever is needed.
     */
    checkValidity?: (payload: SelectValueCheckValidityArgs) => CheckValidityReturn
    /**
     * The objectType that the SelectValue component relates to. This is used in the {@link FindInTrac} component
     * where the user can paste object IDs into the SelectValue component in order to search for them.
     */
    objectType?: trac.metadata.ObjectType
    /**
     * A function that is dispatched that searches for objects based on any object IDs pasted into the SelectValue
     * component.
     */
    getTagsFromValue?: ActionCreatorWithPayload<GetTagsFromValuePayload | string>
    /**
     * A function that is called when getTagsFromValue is running and that can be used to set messages or states in
     * the application while the request is being processed.
     */
    runningGetTagsFromValue?: ActionCreatorWithPayload<{ storeKey: undefined | string, running: boolean }>
    /**
     * A function that runs when the user hits 'enter' on the keyboard while the focus is on the SelectValue input.
     */
    onEnterKeyPress?: (payload: string | number | null) => void
}

export type SelectValueNumberProps = {
    specialType?: undefined
    value: number | null
    basicType: trac.BasicType.FLOAT | trac.BasicType.INTEGER | trac.BasicType.DECIMAL
}

export type SelectValueTextAreaProps = {
    specialType?: "TEXTAREA"
    rows: number
    value: string | null
    basicType: trac.BasicType.STRING
}

export type SelectValueStringProps = {
    specialType?: undefined
    value: string | null
    basicType: trac.BasicType.STRING
}

export type SelectValuePasswordProps = {
    specialType?: "PASSWORD"
    value: string | null
    basicType: trac.BasicType.STRING
}

export type SelectValueProps<U extends void | string | number = void> =
    SelectCommonProps
    & SelectValueExtraProps
    & (U extends void ? SelectValueNumberProps | SelectValueTextAreaProps | SelectValueStringProps | SelectValuePasswordProps : U extends string ? SelectValueTextAreaProps | SelectValueStringProps | SelectValuePasswordProps : U extends number ? SelectValueNumberProps : never)


export type SelectUserProps =
    Pick<SelectOptionProps, "value" | "options" | "storeKey" | "id" | "name" | "isDisabled" | "isDispatched" | "onChange" | "className" | "tooltip" | "labelText" | "labelPosition">
    &
    {
        setCreateOrUpdate: Function
        createOrUpdate: "CREATE" | "UPDATE"
    }

/**
 * A type interface for common props sent back from all select components when their value is changed e.g. SelectOption or SelectValue.
 */
export type SelectCommonPayload =
    Pick<SelectCommonProps, "id" | "name" | "storeKey">
    & Pick<CheckValidityReturn, "isValid">
    & (Pick<SelectDateProps, "basicType" | "value"> | Pick<SelectOptionProps, "basicType" | "value"> | (Pick<SelectToggleProps, "value"> & { basicType: trac.BasicType.BOOLEAN }) | Pick<SelectValueProps, "basicType" | "value">)

/**
 * The payload returned by the Button component. Packet is a generic object that can be passed back to the onClick event.
 */
export  type ButtonPayload = Pick<SelectCommonProps, "id" | "name" | "index"> & { packet?: Record<string, any> }

export type SelectDatePayload = Pick<SelectCommonProps, "id" | "index" | "name" | "storeKey"> &
    Pick<CheckValidityReturn, "isValid"> &
    Pick<SelectDateProps, "basicType" | "value"> &
    { formatted: string | null }

export type SelectOptionPayload<Option, IsMulti extends boolean> = Pick<SelectCommonProps, "id" | "index" | "name" | "storeKey"> &
    Pick<CheckValidityReturn, "isValid"> &
    Pick<SelectOptionProps, "basicType"> &
    /**
     * The selected options.
     */
    { value: IsMulti extends true ? MultiValue<Option> : SingleValue<Option> }

export type SelectValuePayload<U extends void | string | number = void> =
    Pick<SelectCommonProps, "id" | "index" | "name" | "storeKey">
    &
    Pick<CheckValidityReturn, "isValid">
    &
    Pick<SelectValueProps<U>, "basicType" | "value">


export type SelectTogglePayload = Pick<SelectCommonProps, "id" | "index" | "name" | "storeKey"> &
    Pick<CheckValidityReturn, "isValid"> &
    (Pick<SelectToggleProps, "value"> & { basicType: trac.BasicType.BOOLEAN })

// TODO remove the read only part
export type SelectPayload<Option2, IsMulti extends boolean> =
    SelectOptionPayload<Option2, IsMulti>
    | SelectValuePayload
    | SelectTogglePayload
    | SelectDatePayload

/**
 * Size list for Bootstrap, this is not the full list ('xs', 'sm', 'md', 'lg'...) but only for
 * components that have different sizes like buttons. When undefined Bootstrap defaults to 'md'.
 */
export type SizeList = "sm" | "lg"

/**
 * Extra information for options in the {@link SelectOption} component. AsString is used for options such as boolean
 * values where we need a string version of true/false for the UI.
 */
export type AsString =
    { "asString": null | string }

/**
 * Extra information for options in the {@link SelectOption} component. Position is used for options such as units
 * options where we need to know whether the '$' or '%' goes before or after the value.
 */
export type Position =
    { "position": "both" | "pre" | "post" }

/**
 * Extra information for options in the {@link SelectOption} component. Multiplier is used for options such as number
 * units options where we need to know how a particular unit such as "m" needs to be scaled e.g. "m" is 1000000.
 */
export type Multiplier =
    {"multiplier": number}

export type Optional =
    { [key in "optional" | "userAdded"]?: boolean }

export type Names =
    { [key in "jsonName" | "listName"]?: string }

export type OptionDetailsSchema = {
    optional: boolean
    jsonName: string
    listName: string
}

/**
 * Extra information for options in the {@link SelectOption} component. The colour and message to use for each TRAC job status.
 */
export type JobStatusDetails = {
    variant: Variants
    message: string
}

export type RepoDetails = { owner: { html_url: string, login: string }, defaultBranch: undefined | string }

export type CommitDetails = { commit: (ArrayElement<Endpoints["GET /repos/{owner}/{repo}/commits"]["response"]["data"]> & {identifier: "commit"}) | (ArrayElement<Endpoints["GET /repos/{owner}/{repo}/releases"]["response"]["data"]> & { sha: undefined | string, identifier: "release" }), treeSha: string | undefined }

export type CreateDetails = { userAdded: true }

export type NodeDetails = { type: trac.FlowNodeType, isRename: boolean }

export type SchemaDetails = { schema: trac.metadata.IFieldSchema }

export type BasicTypeDetails = { basicTypes: trac.BasicType[] }

// export type Option = {
//     value: string | boolean | number | null,
//     label: string,
//     disabled?: boolean,
//     tag?: trac.metadata.Tag,
//     type?: null | trac.BasicType | trac.ObjectType,
//     details?: Optional | Names | AsString | RepoDetails | CommitDetails
// }

type Type = trac.BasicType | trac.ObjectType | trac.FlowNodeType

type Details =
    Optional
    | Names
    | AsString
    | RepoDetails
    | CommitDetails
    | OptionDetailsSchema
    | CreateDetails
    | Multiplier
    | Position
    | NodeDetails
    | SchemaDetails
    | BasicTypeDetails
    | JobStatusDetails

type Tag = trac.metadata.ITag | trac.metadata.Tag
type TagHeader = trac.metadata.ITagHeader

type TypeObject<U extends Type> = { type: U }
type DetailsObject<U extends Details> = { details: U }
type TagObject<U extends Tag> = { tag: U }
type TagHeaderObject<U extends TagHeader> = { tagHeader: U }

// export type Option<T, P extends void | null | trac.BasicType | trac.ObjectType | Optional | Names | AsString | RepoDetails | CommitDetails = void, U extends void | null | trac.BasicType | trac.ObjectType | Optional | Names | AsString | RepoDetails | CommitDetails = void>
//     = P extends null | trac.BasicType | trac.ObjectType ? NoNullField<BaseOption<T, P, U>> : BaseOption<T, P, U>
export type GenericOption = Option<void | null | boolean | number | string, void | Type | Details | Tag | TagHeader>

export type GenericGroup = { label: undefined | string, options: GenericOption[] }

export type BaseOption<T extends void | null | string | number | boolean | ObjectTypesString> = {
    value: T extends void ? null | string | number | boolean : T
    label: string
    disabled?: boolean
    icon?: "bi-check-circle" | "bi-exclamation-diamond" | "bi-x-circle" | "bi-info-circle"
}

export type Option<T extends void | null | string | number | boolean | ObjectTypesString = void, P extends void | Type | Details | Tag | TagHeader = void>
    = P extends void ? BaseOption<T> : P extends Details ? (BaseOption<T> & DetailsObject<P>) : P extends Type ? (BaseOption<T> & TypeObject<P>) : P extends Tag ? (BaseOption<T> & TagObject<P>) : P extends TagHeader ? (BaseOption<T> & TagHeaderObject<P>) : never

export type IsMulti = boolean

export type Group<T extends void | null | string | number | boolean = void, U extends void | Type | Details | Tag | TagHeader = void> = { label: string, options: Option<T, U>[] }

export type OnCreateNewOptionPayload<U extends CreateDetails | Tag> = {
    basicType: SelectOptionProps["basicType"]
    id: SelectCommonProps["id"]
    name: SelectCommonProps["name"]
    storeKey: SelectCommonProps["storeKey"]
    inputValue: string
    currentOptions: OptionsOrGroups<GenericOption, GenericGroup>,
    newOptions: U extends Tag ? Option<string, Tag>[] : Option<string, CreateDetails>
}

export interface OptionLabels {
    showObjectId: boolean
    showVersions: boolean
    showCreatedDate: boolean
    showUpdatedDate: boolean

}

export type GetTagsFromValuePayload = {
    basicType: SelectValueProps["basicType"]
    id: SelectCommonProps["id"]
    index: SelectCommonProps["index"]
    name: SelectCommonProps["name"]
    storeKey: SelectCommonProps["storeKey"]
    inputValue: string
    tags: trac.metadata.ITag[]
}

export type SelectValueCheckValidityArgs =
    Pick<SelectCommonProps, "mustValidate" | "name" | "id">
    & Pick<SelectValueExtraProps, "minimumValue" | "maximumValue">
    & Pick<SelectValueProps, "value" | "basicType">

export type SelectToggleCheckValidityArgs =
    Pick<SelectCommonProps, "mustValidate" | "name" | "id">
    & Pick<SelectToggleProps, "value">
    & { basicType: trac.BasicType.BOOLEAN }

export type SelectDateCheckValidityArgs =
    Pick<SelectCommonProps, "mustValidate" | "name" | "id">
    & Pick<SelectDateProps, "value" | "minimumValue" | "maximumValue" | "basicType">

export type SelectOptionCheckValidityArgs =
    Pick<SelectCommonProps, "mustValidate" | "name" | "id">
    & Pick<SelectOptionProps, "value" | "basicType" | "isMulti">

export type CheckValidityReturn = {
    isValid: boolean
    message: string
}

/**
 * A type interface for a GitHub API request object. Used when loading models into GitHub for example.
 */
export type GitRequestInit = {
    method: GitHubMethods
    headers: {
        Accept: string
        "Content-Type": string
        "User-Agent": string
        Authorization: string
    }
    body?: string
}

/**
 * A type interface for the request types used by the UI.
 */
export type GitHubMethods = "GET" | "POST" | "PATCH"

export type CodeRepositoryCommon = { application: string, organisation: string }

export type ModelRepository = {
    type: "gitHub"
    name: string
    owner: string
    ownerType: "user" | "organisation"
    modelFileExtensions?: string[]
    modelMetadataExtension?: string
    modelMetadataName?: string
    tracConfigName: string
    tenants: string[]
    httpUrl?: string,
    apiUrl?: string
}

export type DatasetRepository = CodeRepositoryCommon & { data_file_extensions: string[] }

export type FlowRepository = CodeRepositoryCommon & { flow_file_extensions: string[] }

export type CodeRepositories = ModelRepository[]

// export type  CodeRepositories = {
//     models: ModelRepository[]
//     flows: FlowRepository[]
//     datasets: DatasetRepository[]
// }

export type Images = {
    application: { darkBackground: ConfigImage, lightBackground: ConfigImage }
    client: { darkBackground: ConfigImage, lightBackground: ConfigImage }
}
export type Application = { name: string, tagline?: string, maskColour: "purple" | "red" | "blue" | "green", "webpageTitle": string }

/**
 * A type that makes everything in an interface writeable, this is needed as in the SelectOption component
 * some elements of the in built types are read only, however when we addd them to a Redux store they need
 * to be writeable.
 */
export type DeepWritable<T> = { -readonly [P in keyof T]: DeepWritable<T[P]> };

export type ExtractArrayType<T> = T extends (infer U)[] ? U : T;

/**
 * Information from the operating system and provided when a file is loaded up for loading into TRAC. This
 * has information about the use doing the upload.
 */
export type FileSystemInfo = {
    fileExtension: string
    fileName: string
    lastModifiedTracDate: string,
    lastModifiedFormattedDate: string,
    mimeType: string,
    numberOfRows?: number | string,
    sizeAsText: string
    sizeInBytes: number
    userName: string
    userId: string
}

/*Used by the guessVariableTypes function and the FileImportModal component*/
export type GuessedVariableTypes = Record<string, {
    types: { found: (BasicTypesString | "NULL")[], recommended: trac.BasicType[] },
    inFormats: { found: ("NATIVE" | "STRING" | "dd/MM/yyyy" | "yyyy/MM/dd" | "yyyy-MM-dd" | "dd MMMM yyyy" | "dd MMM yyyy" | "QQQ yyyy" | "isoDatetime")[] }
}>

/**
 * An interface for imported csv and Excel file variables schemas, with information about the variable names and what formats have been found for the variable.
 */
export type ImportedFileSchema = trac.metadata.IFieldSchema & { jsonName?: string, inFormat?: string }

export type FileImportModalPayload = {
    data: DataRow[] | Blob | Uint8Array | string
    /*pdf and whole Excel files do not have schemas*/
    schema?: ImportedFileSchema[],
    fileInfo: FileSystemInfo,
    /*pdf and whole Excel files do not have guessed schemas*/
    guessedVariableTypes?: GuessedVariableTypes,
    wholeFile: boolean
    file?: File
}

export type TracGroup = [
    { label: "Added by user", options: SearchOption[] },
    { label: "Loaded for re-run", options: SearchOption[] },
    { label: "Search results", options: SearchOption[] },
]

export type FlowGroup = [
    { label: "Models", options: Option<string, NodeDetails>[] },
    { label: "Inputs", options: Option<string, NodeDetails>[] },
    { label: "Outputs", options: Option<string, NodeDetails>[] },
    { label: "Intermediates", options: Option<string, NodeDetails>[] },
]

export type BusinessSegmentGroup = { label: undefined | string, options: Option<string>[] }[]

export type SearchOption = Option<string, trac.metadata.ITag>


export type MetadataTableToShow = { key: keyof trac.metadata.ITag, title?: string }


/*FLOW SVG*/

/**
 * A function that gets an array of the renamed datasets in the flow.
 */
export type RenamedDataset = { datasetStart: string, datasetEnd: string, modelStart: string, modelEnd: string }

export type LabelSizes = { [key: string]: { height: number, width: number } }

export type GregsEdge = {
    id: string,
    sources: string[]
    targets: string[]
    start: { node: string, type: trac.FlowNodeType }
    end: { node: string, type: trac.FlowNodeType }
    isRename?: boolean
}

export type GraphLayout = {
    g: {
        children?: (ElkNode & { type?: trac.FlowNodeType })[]
        ports?: ElkPort[]
        edges?: (Partial<ElkExtendedEdge> & { isRename?: boolean })[]
    }
    dimensions: {
        heightRatio: number
        widthRatio: number
        containerWidth: number
        containerHeight: number
        xOffset: number
        yOffset: number
        modifiedHeight: number
        modifiedWidth: number
        originalWidth: number
        originalHeight: number
    }
}

export type KeyLabels = {
    input: string
    output: string
    model: string
    intermediate: string
}

export type ObjectDictionary = { [key in trac.ObjectType]: { to: string } }

export type ChartType =
    "line"
    | "lineWithError"
    | "area"
    | "scatter"
    | "arearange"
    | "spline"
    | "areaspline"
    | "column"
    | "columnWithError"
    | "bar"
    | "pie"
    | "circleDoughnut"
    | "semiDoughnut"
    | "histogram"
    | "bubble"
    | "sankey"
    | "dependencywheel"
    | "waterfall"

export type HighchartChartType = "area" |
    "arearange" |
    "areaspline" |
    "bar" |
    "bubble" |
    "bullet" |
    "column" |
    "columnrange" |
    "dependencywheel" |
    "histogram" |
    "line" |
    "pie" |
    "sankey" |
    "scatter" |
    "solidgauge" |
    "spline" |
    "waterfall"

export type ChartAxisDirection = "x" | "y" | "z" | "e"

export type ChartAxes = "x" | "y1" | "y2" | "z" | "e1" | "e2"

export type ChartDefinition = {
    xAxisFieldTypes: trac.BasicType[]
    yAxisFieldTypes: trac.BasicType[]
    zAxisFieldTypes: trac.BasicType[]
    eAxisFieldTypes?: trac.BasicType[]
    maxSeriesPerAxis: Record<ChartAxes, number>
    defaultNumberOfSeriesSelected: Record<ChartAxes, number>
    selectorLabels?: { [key in ChartAxes]?: string }
    categorical?: ChartAxisDirection[],
}

// /**
//  * An interface for the {@link useChart} custom hook.
//  */
// export type UseChartPayload =
//     Pick<FullChartState, "allowAllSegmentsToBePlotted" | "chartType" | "defaultSegmentationVariables" | "defaultVariables" | "userSetAxisLabels" | "userSetVariableLabels"> &
//     { chartDefinition: ChartDefinition, data: DataRow[], fields: trac.metadata.IFieldSchema[], tag?: trac.metadata.ITag }

/**
 * An interface for the information added into series in the Chart component, so we can correctly format the
 * axis labels and show the correct axis types.
 */
export interface CustomSeriesData {
    fieldNameY?: null | string,
    formatCodeY?: null | string,
    fieldTypeY?: null | trac.BasicType,
    formatCodeX?: null | string,
    fieldTypeX?: null | trac.BasicType
    // This is used to format the series symbol in the chart legend
    customShowRectSymbol?: boolean
}

// /**
//  * An interface for the variables created by the {@link useChart} custom hook. This contains both the variable
//  * options for each axis in a chart and the segmentation to apply, it also contains the selected options for both.
//  */
// export interface UseChartOptionsAndSelections {
//     options: {
//         axes: Record<ChartAxisDirection, (Option<string, SchemaDetails>)[] | (Group<string, SchemaDetails>[])>
//         segments: { fields: trac.metadata.IFieldSchema[], options: Map<string, Option[]> }
//     }
//     selected: {
//         axes: Record<"y1" | "y2", null | Option<string, SchemaDetails> | (Option<string, SchemaDetails>)[]> & Record<"x" | "e1" | "e2" | "z", null | Option<string, SchemaDetails>>
//         segments: Record<string, null | Option>
//         fieldNamesInChart: string[]
//     }
//     labels: { userSetVariableLabels: Exclude<UseChartPayload["userSetVariableLabels"], undefined>, userSetAxisLabels: Exclude<UseChartPayload["userSetAxisLabels"], undefined> }
// }

// /**
//  * An interface for the worker.postMessage function in the {@link workerToLoadData} function.
//  */
// export type UseWorkerToLoadDataMessage = Pick<UseWorkerToLoadDataPayload, "data" | "xVariables" | "y1Variables" | "y2Variables" | "series" | "segmentationVariables" | "selectedSegmentFilters">

// /**
//  * An interface for the worker.postMessage function in the {@link workerToGetSegmentOptions} function.
//  */
// export type UseWorkerToGetSegmentOptionsMessage = Pick<UseWorkerToGetSegmentOptionsPayload, "data" | "segmentationVariables" | "selectedSegmentFilters">
//
// /**
//  * An interface for the set of functions created by the {@link useChart} custom hook. These allow the user to set the
//  * variable selections for each axis in a chart and the segmentation to apply.
//  */
// export type UseChartFunctions = {
//     setAxisVariables: (payload: UseChartOptionsAndSelections["selected"]["axes"]) => void
//     setSegmentOptions: (payload: UseChartOptionsAndSelections["options"]["segments"]["options"]) => void
//     setSelectedSegmentFilters: (payload: UseChartOptionsAndSelections["selected"]["segments"], info?: { dataIsFiltered: boolean, dataIsSampled: boolean }) => void
//     setUserSetLabels: (payload: UseChartOptionsAndSelections["labels"]) => void
// }

/**
 * An interface of additional information that comes back from the {@link addSeriesData} function.
 */
export interface ChartDataProcessingInfo {
    dataIsFiltered: boolean,
    dataIsSampled: boolean
}

export type ColourOptions =
    "lowHeatmapColour"
    | "highHeatmapColour"
    | "lowTrafficLightColour"
    | "highTrafficLightColour"
    | "transitionTrafficLightColour"
export type ColourColumnTypes = "none" | "heatmap" | "trafficlight"

/**
 * An interface for defining the heatmap or traffic light colours to use for a column. This is used by the {@link Table}
 * component.
 */
export type ColumnColourDefinition = {
    type: ColourColumnTypes,
    basicType: trac.BasicType,
    minimumValue: undefined | number | string | boolean,
    maximumValue: undefined | number | string | boolean,
    transitionValue: null | number | string | boolean,
    lowColour: string,
    highColour: string,
    transitionColour: string
    /**
     * The lowTextColour, highTextColour, transitionTextColour are fixed for traffic
     * light columns but are calculated per cell for heat maps. For the former we
     * calculate the value once and store it here rather than calculate the same
     * values for every cell.
     */
    lowTextColour?: "white" | "black"
    highTextColour?: "white" | "black"
    transitionTextColour?: "white" | "black"
}

/**
 * An object containing interface for defining the heatmap or traffic light colour mappings keyed by the column
 * names in a dataset.
 */
export type ColumnColourDefinitions = Record<string, ColumnColourDefinition>

// A type for a ref that stores the heights of various utility rows in the table
export type TableRowHeights = Record<"head" | "columnOrder" | "columnFilter", number>

export type TracUiTableState = {
    showGlobalFilter: boolean
    height: string
    showInformation: boolean
    paginate: boolean
    columnColours: ColumnColourDefinitions
    showColumnFilters: boolean
    showColumnOrder: boolean
    rowHeights: TableRowHeights
}

export type FullTableState = Partial<{
    tracUiState: TracUiTableState,
    reactTableState: InitialTableState
}>


/**
 * An object with the information about a folder from GitHub and additional information used by the @{link FileTree} component
 * for folder navigation.
 */
export type FolderTree = Record<string, (ArrayElement<Endpoints["GET /repos/{owner}/{repo}/git/trees/{tree_sha}"]["response"]["data"]["tree"]> & { selectable: boolean })>

/**
 * An object with the information about a model .py file from GitHub and additional information used by the @{link FileTree} component
 * to decide whether the file can be selected.
 */
export type ModelFile = (ArrayElement<Endpoints["GET /repos/{owner}/{repo}/git/trees/{tree_sha}"]["response"]["data"]["tree"]> & { selectable: boolean, fileExtension: string, location: string })


/**
 * An interface for the file tree object, when we get the file tree from GitHub it is an array of entries, we map it to a recursive object
 * to make it easier to make it navigable.
 *
 * Note that the definition below is a recursive Typescript definition, see https://stackoverflow.com/questions/47842266/recursive-types-in-typescript
 */
export interface FileTree extends FileTreeNode<FileTree> {
}

/**
 * A node in the recursive file tree.
 */
export type FileTreeNode<T> = Record<string, ModelFile | T | (ModelFile & T)>


export type PopupDetails = {
    selectable: {
        path: string,
        show: boolean
    },
    unselectable: {
        path: string,
        show: boolean
    }
}

export type SelectedFileDetails = Pick<ArrayElement<Endpoints["GET /repos/{owner}/{repo}/git/trees/{tree_sha}"]["response"]["data"]["tree"]>, "size" | "path" | "sha"> & { selectable: boolean, fileExtension: string }

export type FileImportTypes = ("xlsx" | "csv" | "pdf")[]

export type ValidationOfModelsAndFlowData = Record<string, Record<"flow" | "models", { missingInputs: string[], missingOutputs: string[] }> & { modelName: string }>

export type ValidationOfModelsParameters = { key: string, types: trac.metadata.BasicType[], label: null | string }[]

export type ValidationOfModelInputs = Record<string, { modelKey?: string, modelName?: string, missing: string[], type: { fieldName: string, want: trac.BasicType, have: trac.BasicType }[] }>

export type ConfirmButtonPayload = { id?: number | string, name?: number | string, index?: number }

export type QueryButton = {
    value: string
    label?: string
    tooltip: string
    icon?: string
    outputLabelStart?: string
    outputLabelEnd?: string
    fieldTypes: trac.BasicType[],
    outputType?: "INHERIT" | trac.BasicType
    outputFormat?: string
    outputCategorical?: boolean | "INHERIT"
}

export type UpdateSchemaPayload = {
    index: number
    fieldSchema: trac.metadata.IFieldSchema
    fieldOrder?: { fieldOrderToSwapWith: number, oldFieldOrder: number }
}

/**
 * An interface for values extracted from TRAC metadata header and attribute properties.
 */
export type ExtractedTracValue = null | string | number | boolean | Record<string, string | number | boolean> | (string | number | boolean)[]

/**
 * An interface for the types of input datasets in a flow, extracted from TRAC flow definitions this is used in the {@link RunAFlowScene}
 * to layout the input dataset menu.
 */
export interface JobInputsByCategory {
    required: string[],
    optional: string[]
    categories: Record<string, string[]>
}

/**
 * A function that takes a value and tries to determine what type of variable it is, this is relatively straight forward when
 * the value is a native Javascript type e.g. a number, but when the variable is a string we also try and work out if the
 * variable is another type stored as a string e.g. "1.2" would be a number.
 *
 * @param variable - The variable to guess the type of.
 * @returns An object containing the guessed type as a string and the format that was found.
 */

export interface GuessVariableType {
    inFormat: "NATIVE" | "STRING" | "dd/MM/yyyy" | "yyyy/MM/dd" | "yyyy-MM-dd" | "dd MMMM yyyy" | "dd MMM yyyy" | "QQQ yyyy" | "isoDatetime" | null,
    type: "BOOLEAN" | "DATE" | "DATETIME" | "NULL" | "INTEGER" | "STRING" | "FLOAT/DECIMAL"
}

/**
 * An interface for the streaming update lifecycle function arguments (e.g. onError) that allow updates to the user to be shown.
 */
export interface StreamingPayload {

    completed: number,
    duration?: number,
    message: string,
    toDo: number,
}

/**
 * An interface for the streaming update event functions passed as a payload to the streaming functions.
 */
export interface StreamingEventFunctions {
    onStart: ({toDo, completed, message, duration}: StreamingPayload) => void,
    onProgress: ({toDo, completed, message, duration}: StreamingPayload) => void,
    onComplete: ({toDo, completed, message, duration}: StreamingPayload) => void,
    onError: ({toDo, completed, message, duration}: StreamingPayload) => void
}

/**
 * The result returned from streaming uploads of data.
 */
export interface StreamingDataResult {
    tag: trac.metadata.ITagHeader,
    duration: number,
    numberOfRows: number
}

export type ListsOrTabs = "lists" | "tabs"

/**
 * Get the array element type from an array interface.
 * @See https://stackoverflow.com/questions/41253310/typescript-retrieve-element-type-information-from-array-type
 */
export type ArrayElement<ArrayType extends readonly unknown[]> =
    ArrayType extends readonly (infer ElementType)[] ? ElementType : never;
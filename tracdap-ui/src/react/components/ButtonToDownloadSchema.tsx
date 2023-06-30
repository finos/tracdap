/**
 * A component that shows a button that when you click on it downloads a schema as a CSV file.
 * If text is provided for the button then the component shows a button with an icon and text,
 * if no text is provided then just an CSV file icon is shown.
 *
 * @module ButtonToDownloadSchema
 * @category Component
 */

import {Button} from "./Button";
import {downloadDataAsCsvFromJson} from "../utils/utils_downloads";
import {Icon} from "./Icon";
import PropTypes from "prop-types";
import React from "react";
import type {DataRow, SizeList} from "../../types/types_general";
import {tracdap as trac} from "@finos/tracdap-web-api";
import {useAppSelector} from "../../types/types_hooks";
import {fillSchemaDefaultValues} from "../utils/utils_schema";
import {convertBasicTypeToString} from "../utils/utils_trac_metadata";

/**
 * An interface for the props of the Button component.
 */
export interface Props {

    /**
     * The css class to apply to the button, this allows additional styles to be added to the component.
     */
    className?: string
    /**
     * Whether the button should be disabled, this means that the state of the button can be
     * managed by the parent component while the tag loads from the API.
     */
    loading?: boolean
    /**
     * The bootstrap size for the button.
     */
    size?: SizeList
    /**
     * The object's metadata tag, this is the schema that will be downloaded.
     */
    tag?: null | trac.metadata.ITag
    /**
     * The text for the button.
     */
    text?: string,
}

export const ButtonToDownloadSchema = (props: Props) => {

    const {className = "", loading = false, size, tag, text} = props

    // Get what we need from the store
    const {userId} = useAppSelector(state => state["applicationStore"].login)

    // This takes the fields for the schema and adds in the default values that are not sent via the API.
    // So adds in the defaults for categorical, field order etc. We also convert the field type from an enum
    // and into a humanreadable version.
    const schemaWithDefaults = tag?.definition?.schema?.table?.fields?.map(field => {

        // We need to copy to remove the IFieldSchema interface, the copy below removes the methods
        const fieldWithDefaults : DataRow = {...fillSchemaDefaultValues(field)}

        fieldWithDefaults.fieldType = convertBasicTypeToString(field.fieldType ?? trac.BasicType.BASIC_TYPE_NOT_SET, false)

        return fieldWithDefaults
    })

    return (

        <Button ariaLabel={"Download schema"}
                className={`min-width-px-30 ${!text ? "p-0 m-0" : ""} ${className}`}
                disabled={!tag}
                size={size}
                isDispatched={false}
                loading={loading}
                onClick={() => tag && schemaWithDefaults && downloadDataAsCsvFromJson(schemaWithDefaults, tag, userId)}
                variant={!text ? "link" : "info"}
        >
            <Icon ariaLabel={false}
                  icon={!text ? "bi-filetype-csv" : "bi-file-arrow-down"}
                  size={!text ? "2rem" : undefined}
                  className={!text ? undefined : "me-2"}

            />
            {text || null}
        </Button>
    )
};

ButtonToDownloadSchema.propTypes = {

    className: PropTypes.string,
    loading: PropTypes.bool,
    size: PropTypes.oneOf(['lg', 'sm']),
    tag: PropTypes.object,
    text: PropTypes.string
};
/**
 * A component that shows a small table of information relating to a file loaded from the local file system. This is
 * used to display information when the user is loading a file into TRAC from the FileImportModal component.
 *
 * @module FileDetails
 * @category Component
 */

import {convertKeyToText} from "../utils/utils_string";
import type {FileSystemInfo} from "../../types/types_general";
import PropTypes from "prop-types";
import React from "react";
import Table from "react-bootstrap/Table";

// The default for the fieldsToNotShow & keyText props. This is defined outside the component
// in order to prevent re-renders.
const defaultFieldsToNotShow = ["sizeInBytes", "userName", "lastModifiedTracDate"]
const defaultKeyText = {lastModifiedFormattedDate: "Last modified date", sizeAsText: "Size"}

/**
 * An interface for the props of the FileDetails component.
 */
export interface Props {

    /**
     * An array of the properties of the fileInfo object to not show in the table We do this as some
     * items are used to add attributes to the file in TRAC, but we do not want to show them to the user
     * in their raw form (usually a human-readable variant is also available).
     */
    fieldsToNotShow?: keyof FileSystemInfo,
    /**
     * An object containing information about a file and to show in a table.
     */
    fileInfo: FileSystemInfo,
    /**
     * An object that can be used to override the label given to a property in the table, instead of the using the key
     * of the file fileInfo object as the text for the label. By default, the fileInfo property keys are long hand
     * variable names that may not be appropriate to show the user.
     */
    keyText?: Record<string, string>
}

export const FileDetails = (props: Props) => {

    const {
        fieldsToNotShow = defaultFieldsToNotShow,
        fileInfo,
        keyText = defaultKeyText
    } = props;

    return (
        <Table striped size={"sm"}>
            <tbody>

            {Object.entries(fileInfo).filter(([key]) => !fieldsToNotShow.includes(key)).map(([key, value]) =>

                <tr key={key}>
                    <td className={"text-nowrap"}>{keyText[key] || convertKeyToText(key)}:</td>
                    <td className={"user-select-all"}>{value}</td>
                </tr>
            )}

            </tbody>
        </Table>
    );
};

FileDetails.propTypes = {

    fieldsToNotShow: PropTypes.arrayOf(PropTypes.string),
    fileInfo: PropTypes.shape({
        fileExtension: PropTypes.string.isRequired,
        fileName: PropTypes.string.isRequired,
        lastModifiedFormattedDate: PropTypes.string.isRequired,
        lastModifiedTracDate: PropTypes.string.isRequired,
        userName: PropTypes.string.isRequired,
        userId: PropTypes.string.isRequired,
        sizeInBytes: PropTypes.number.isRequired,
        sizeAsText: PropTypes.string.isRequired
    }).isRequired,
    keyText: PropTypes.object
};
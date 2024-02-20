/**
 * A component that shows a button that when you click on it downloads a json file of an object's metadata.
 * The component does not make API calls to get information required. If text is provided for the button then
 * the component shows a button with an icon and text, if no text is provided then just an JSON file icon is shown.
 *
 * @module ButtonToDownloadJson
 * @category Component
 */

import {Button} from "./Button";
import {Icon} from "./Icon";
import {makeJsonDownload} from "../utils/utils_downloads";
import PropTypes from "prop-types";
import React from "react";
import type {SizeList} from "../../types/types_general";
import {tracdap as trac} from "@finos/tracdap-web-api";
import {useAppSelector} from "../../types/types_hooks";

/**
 * An interface for the props of the ButtonToDownloadJson component.
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
     * The object's metadata tag, this is the object that will be downloaded.
     */
    tag?: null | trac.metadata.ITag
    /**
     * The text for the button.
     */
    text?: string,
}

export const ButtonToDownloadJson = (props: Props) => {

    const {className = "", loading = false, size, text, tag} = props

    // Get what we need from the store
    const {userId} = useAppSelector(state => state["applicationStore"].login)

    return (

        <Button ariaLabel={"Download JSON"}
                className={`min-width-px-30 ${!text ? "p-0 m-0" : ""} ${className}`}
                disabled={!tag}
                isDispatched={false}
                loading={loading}
                onClick={() => tag && makeJsonDownload(tag, userId)}
                size={size}
                variant={!text ? "link" : "info"}
        >
            <Icon ariaLabel={false}
                  className={!text ? undefined : "me-2"}
                  icon={!text ? "bi-filetype-json" : "bi-file-arrow-down"}
                  size={!text ? "2rem" : undefined}
                  tooltip={"Download metadata as JSON"}
            />
            {text || null}
        </Button>
    )
}

ButtonToDownloadJson.propTypes = {

    className: PropTypes.string,
    loading: PropTypes.bool,
    size: PropTypes.oneOf(['lg', 'sm']),
    tag: PropTypes.object,
    text: PropTypes.string
};
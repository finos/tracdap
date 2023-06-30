/**
 * A component that shows a button that when you click on it opens up the user's email client
 * and pre-populates an email with a link to an object in TRAC.
 *
 * @module ButtonToEmailObjectLink
 * @category Component
 */

import {Button} from "./Button";
import {convertObjectTypeToString} from "../utils/utils_trac_metadata";
import {Icon} from "./Icon";
import {ObjectSummaryPaths} from "../../config/config_menu";
import PropTypes from "prop-types";
import React from "react";
import type {SizeList} from "../../types/types_general";
import {tracdap as trac} from "@finos/tracdap-web-api";
import {useAppSelector} from "../../types/types_hooks";

/**
 * An interface for the props of the ButtonToEmailObjectLink component.
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
     * The object's metadata selector tag, this is the information that will be .
     */
    tagSelector?: null | trac.metadata.ITagSelector
    /**
     * The text for the button.
     */
    text?: string,
}

export const ButtonToEmailObjectLink = (props: Props) => {

    const {className = "", loading = false, size, tagSelector, text} = props

    // Get what we need from the store
    const {"trac-tenant": tenant} = useAppSelector(state => state["applicationStore"].cookies)
    const {tenantOptions} = useAppSelector(state => state["applicationStore"])

    const objectTypeAsString = convertObjectTypeToString(tagSelector?.objectType ?? trac.ObjectType.OBJECT_TYPE_NOT_SET, true, true)
    const tenantName = tenantOptions.find(tenantOption => tenantOption.value === tenant)?.label
    const subject = `Emailing link to ${objectTypeAsString} in TRAC (${tagSelector?.objectId})`

    // Get the URL and split into parts
    let windowUrl : string | (string)[]= window.location.href.split("/")

    // We need to take everything after "app" off and add on the information we need for the link
    const appIndex = windowUrl.findIndex(windowUrlItem => windowUrlItem === "app")
    windowUrl = window.location.href.split("/").filter((windowUrlItem, i) => appIndex !== undefined && i < appIndex).join("/")

    // This is the URL to use for the link, it has the object and tag versions in it
    const url = `${windowUrl}/${ObjectSummaryPaths[tagSelector?.objectType ?? trac.ObjectType.OBJECT_TYPE_NOT_SET].to }/${tagSelector?.objectId}/${tagSelector?.objectVersion}/${tagSelector?.tagVersion}`

    const body = `${url}\n\nThis ${objectTypeAsString} is stored under the ${tenantName ?? tenant} ${tenantName ? `(${tenant})` : ""} tenant.`

    return (

        <Button ariaLabel={"Email link"}
                className={`min-width-px-30 ${!text ? "p-0 m-0" : ""} ${className}`}
                disabled={!tagSelector}
                href={`mailto:?subject=${encodeURIComponent(subject)}&body=${encodeURIComponent(body)}`}
                size={size}
                isDispatched={false}
                loading={loading}
                variant={!text ? "link" : "info"}
        >
            <Icon ariaLabel={false}
                  className={!text ? undefined : "me-2"}
                  icon={!text ? "bi-envelope" : "bi-file-arrow-down"}
                  size={!text ? "2rem" : undefined}
                  tooltip={"Share link"}
            />
            {text || null}
        </Button>
    )
};

ButtonToEmailObjectLink.propTypes = {

    className: PropTypes.string,
    loading: PropTypes.bool,
    size: PropTypes.oneOf(['lg', 'sm']),
    tagSelector: PropTypes.object,
    text: PropTypes.string
};
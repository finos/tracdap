/**
 * A component that shows a series of badges which can be generated from a string split by a delimiter, an array
 * or an object of key value pairs. These options align to the TRAC attribute basic types including map and array types.
 *
 * @module Badges
 * @category Component
 */

import Badge from "react-bootstrap/Badge";
import {convertAnythingToStringArray} from "../utils/utils_general";
import {convertKeyToText} from "../utils/utils_string";
import type {DataValues, Variants} from "../../types/types_general";
import PropTypes from "prop-types";
import React from "react";

/**
 * Interface for the props of the Badges component.
 */
export interface Props {

    /**
     * Whether to convert text in the badges into a more humanreadable version. In some cases the props passed to this
     * component can already have been processed, so we don't want to do it again here, in other cases we want to use
     * this component to try and present the information better.
     */
    convertText: boolean
    /**
     * The delimiter to use to split the text prop if the text is a string that needs to be converted to an array of badges.
     * @defaultValue '|'
     */
    delimiter?: string
    /**
     * The string, array or object to show as a series of badges.
     */
    text: null | string | (DataValues)[] | Record<string, DataValues>
    /**
     * The Bootstrap variant to show the badges as (this changes the colour).
     * @defaultValue 'light'
     */
    variant?: Variants
}

export const Badges = (props: Props) => {

    const {
        convertText,
        delimiter = "|",
        text,
        variant = "light"
    } = props;

    // Make the list of badges unique
    const textArray = [...new Set(convertAnythingToStringArray(text, delimiter))]

    return (

        <div className={textArray.length > 1 ? "mt-1" : ""}>
            {/*A single item has a minimum width, lists do not*/}
            {textArray.map((item, i) => <Badge key={item} pill
                                               className={`${variant === "light" ? "text-dark" : ""} ${textArray.length === 1 ? "min-width-px-80" : ""} ${textArray.length > 1 ? "mb-1" : ""} ${textArray.length > 0 && i + 1 < textArray.length ? "me-1" : ""}`}
                                               bg={variant}>{convertText ? convertKeyToText(item): item}</Badge>)}
        </div>
    )
};

Badges.propTypes = {

    convertText: PropTypes.bool.isRequired,
    delimiter: PropTypes.string,
    // The PropTypes.number is used for null
    text: PropTypes.oneOfType([
        PropTypes.number,
        PropTypes.string,
        PropTypes.arrayOf(
            PropTypes.oneOfType([
                PropTypes.bool,
                PropTypes.number,
                PropTypes.string,
            ])),
        PropTypes.objectOf(PropTypes.oneOfType([
            PropTypes.bool,
            PropTypes.number,
            PropTypes.string,
        ]))]).isRequired,
    variant: PropTypes.string
};
/**
 * A component that shows an icon based on whether a user selection matches with the policy being applied.
 *
 * @module PolicyStatusIcon
 * @category Component
 */

import PropTypes from "prop-types";
import React from "react";
import {Icon} from "./Icon";

/**
 * An interface for the props of the PolicyStatusIcon component.
 */
export interface Props {

    /**
     * Whether the policy includes a value that the item should have.
     */
    coveredInPolicy: boolean
    /**
     * Whether the value is the same as the policy, can be anything such as a primitive value to and array of options or a TRAC object.
     */
    isValueAlignedWithPolicy: undefined | boolean
}

export const PolicyStatusIcon = (props: Props) => {

    const {coveredInPolicy, isValueAlignedWithPolicy} = props

    return (

        coveredInPolicy ?
            <Icon ariaLabel={isValueAlignedWithPolicy ? "In policy" : "Not in policy"}
                  className={isValueAlignedWithPolicy ? "text-success" : "text-danger"}
                  icon={isValueAlignedWithPolicy ? "bi-check-circle" : "bi-x-circle"}
                  tooltip={isValueAlignedWithPolicy ? "In policy" : "Not in policy"}
            />
            : <span>Not set in policy</span>
    )
};

PolicyStatusIcon.propTypes = {

    coveredInPolicy: PropTypes.bool.isRequired,
    isValueAlignedWithPolicy: PropTypes.bool.isRequired
};
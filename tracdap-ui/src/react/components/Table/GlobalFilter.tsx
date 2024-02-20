/**
 * A component that shows a widget to use to filter all visible columns in a table.
 *
 * @module GlobalFilter
 * @category Component
 */

import Form from "react-bootstrap/Form";
import PropTypes from "prop-types";
import React, {memo, startTransition, useEffect, useState} from "react";

/**
 * An interface for the props of the GlobalFilter component.
 */
export interface Props {

    /**
     * The initial value of the filter when the component mounts, if this is updated then the component updates to the
     * new value.
     */
    initialValue: string
    /**
     * The function to run when the global filter value is changed by the user.
     */
    onChange: (value: string) => void
}

const GlobalFilterInner = (props: Props) => {

    const {
        initialValue,
        onChange
    } = props

    /**
     * A local state variable that stores the value of the global filter, if we do this here then changing it does
     * not trigger an update in the parent.
     */
    const [value, setValue] = useState(initialValue)

    /**
     * A hook that makes sure that the initialValue prop and the value held in state in this component remain in
     * sync if the prop changes after the component has mounted.
     */
    useEffect(() => {
        setValue(initialValue)
    }, [initialValue])

    /**
     * A hook that runs when the user search string changes, the onChange function is set to be de-prioritised,
     * this is useful if the user is typing, and we want to reduce the number of expensive re-renders on
     * the table.
     */
    useEffect(() => {

        // setTransition is a React 18 feature that allows us to say that updating the table is an un-prioritised
        // update whereas updating the input box is prioritised.
        startTransition(() => {
            onChange(value)
        })

    }, [onChange, value])

    return (
        <div className="d-flex">
            <Form.Control type="search"
                          placeholder="Search all columns"
                // min width allows object Ids to be pasted an seen in one go
                          className="ms-2 me-4 min-width-px-300 border-top-0 border-start-0 border-end-0 border-bottom-2"
                          aria-label="Search"
                          value={value}
                          onChange={e => setValue(e.target.value || "")}
            />
        </div>
    )
}

GlobalFilterInner.propTypes = {
    initialValue: PropTypes.string.isRequired,
    onChange: PropTypes.func.isRequired
};

export const GlobalFilter = memo(GlobalFilterInner);
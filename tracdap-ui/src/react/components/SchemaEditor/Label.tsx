/**
 * A component that allows the user to set a label for a field in the schema editor.
 *
 * @module Label
 * @category Component
 */

import PropTypes from "prop-types";
import React, {startTransition, useEffect, useState} from "react";
import {SelectValue} from "../SelectValue";
import type {SelectValuePayload} from "../../../types/types_general";
import {tracdap as trac} from "@finos/tracdap-web-api";

/**
 * An interface for the props of the Label component.
 */
export interface Props {

    /**
     * Whether the fieldName can be edited. When creating a schema you want to be able to edit it but not when
     * editing the schema of a dataset loaded by CSV or Excel.
     */
    canEditFieldName: boolean
    /**
     * The name of the field in the schema.
     */
    fieldName: trac.metadata.IFieldSchema["fieldName"]
    /**
     * The position of the field in the schema.
     */
    index: number
    /**
     * The name of the field in the schema.
     */
    label: trac.metadata.IFieldSchema["label"]
    /**
     * The function to run when the field schema is changed. This updates the edited property in the parent.
     * This will either be in a store or a state of the parent component.
     */
    onChange: (payload: SelectValuePayload<string>) => void
}

export const Label = (props: Props) => {

    const {
        canEditFieldName,
        fieldName,
        index,
        label: initialLabel,
        onChange
    } = props

    /**
     * A local state variable that stores the value of the label, if we do this here then changing it does
     * not trigger an update in the parent.
     */
    const [payload, setPayload] = useState<SelectValuePayload<string> | null>(null)

    /**
     * A hook that makes sure that the initialValue prop and the value held in state in this component remain in
     * sync if the prop changes after the component has mounted.
     */
    useEffect(() => {
        setPayload({basicType: trac.STRING, id: fieldName, value: initialLabel || null, index: index, isValid: false})
    }, [fieldName, index, initialLabel])

    /**
     * A hook that runs when the user label changes, the onChange function is set to be de-prioritised,
     * this is useful if the user is typing, and we want to reduce the number of expensive re-renders on
     * the table.
     */
    useEffect(() => {

        // setTransition is a React 18 feature that allows us to say that updating the table is an un-prioritised
        // update whereas updating the input box is prioritised.
        startTransition(() => {
            if (payload != null && payload) {
                onChange(payload)
            }
        })
    }, [onChange, payload])

    return (

        <div className={"d-flex align-items-end w-md-50 w-lg-25 ps-1 pe-2"}>
            <SelectValue basicType={trac.STRING}
                          className={canEditFieldName ? "flex-fill" : undefined}
                         id={fieldName}
                         index={index}
                         labelText={canEditFieldName ? "Label:" : undefined}
                         labelPosition={"top"}
                         onChange={setPayload}
                         name={"label"}
                         value={payload?.value || null}
                         validateOnMount={false}
                         isDispatched={false}
            />
        </div>
    )
};

Label.propTypes = {

    canEditFieldName: PropTypes.bool.isRequired,
    onChange: PropTypes.func.isRequired,
    fieldName: PropTypes.string.isRequired,
    index: PropTypes.number.isRequired,
    label: PropTypes.string
};
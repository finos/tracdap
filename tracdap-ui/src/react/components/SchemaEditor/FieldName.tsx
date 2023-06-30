/**
 * A component that shows the name of a field in the schema editor.
 *
 * @module FieldName
 * @category Component
 */

import {ConfirmButton} from "../ConfirmButton";
import {Icon} from "../Icon";
import PropTypes from "prop-types";
import React from "react";
import {tracdap as trac} from "@finos/tracdap-web-api";
import {SelectValue} from "../SelectValue";
import type {SelectValuePayload} from "../../../types/types_general";

/**
 * An interface for the props of the FieldName component.
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
     * The function to run when the field schema is changed. This updates the edited property in the parent.
     * This will either be in a store or a state of the parent component.
     */
    onChange: (payload: SelectValuePayload<string>) => void
}

export const FieldName = (props: Props) => {

    const {canEditFieldName, fieldName, index, onChange} = props

    return (

        <React.Fragment>
            {canEditFieldName &&
                <div className={"d-flex ps-1 pe-2"}>
                    <SelectValue basicType={trac.STRING}
                                 className={"w-50"}
                                 id={fieldName}
                                 index={index}
                                 labelText={"Name:"}
                                 labelPosition={"top"}
                                 onChange={onChange}
                                 mustValidate={true}
                                 name={"fieldName"}
                                 showValidationMessage={true}
                                 value={fieldName || null}
                                 validateOnMount={true}
                                 isDispatched={false}
                    />

                    <ConfirmButton ariaLabel={"Delete field"}
                                   cancelText={"No"}
                                   className={"ms-3 px-0 flex-fill text-end"}
                                   confirmText={"Yes"}
                                   description={"Are you sure that you want to delete this field?"}
                                   onClick={() => {}}
                                   index={index}
                                   variant={"link"}
                    >
                        <Icon ariaLabel={false}
                              icon={'bi-trash3'}
                        />
                    </ConfirmButton>

                </div>
            }
            {!canEditFieldName &&
                <div className={"fs-13 ps-1"}>
                    <strong>{fieldName}</strong>
                </div>
            }
        </React.Fragment>

    )
};

FieldName.propTypes = {

    canEditFieldName: PropTypes.bool.isRequired,
    fieldName: PropTypes.string.isRequired,
    index: PropTypes.number.isRequired
};
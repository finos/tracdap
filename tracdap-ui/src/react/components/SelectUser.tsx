/**
 * A component that allows the user to select a username. It is based on the SelectOption component which uses the
 * react-select package. The ability to select a user by name is combined with the ability to set whether this should be
 * the 'created by' or the 'updated by' user, this is useful for example when searching for objects in TRAC.
 *
 * @module SelectUser
 * @category Component
 */

import Form from "react-bootstrap/Form";
import {LabelLayout} from "./LabelLayout";
import PropTypes from "prop-types"
import React, {memo} from "react";
import {SelectOption} from "./SelectOption";
import {SelectUserProps} from "../../types/types_general";
import {tracdap as trac} from "@finos/tracdap-web-api";
import {useAppDispatch} from "../../types/types_hooks";

const SelectUserInner = (props: SelectUserProps) => {

    const {
        createOrUpdate,
        id,
        isDisabled = false,
        isDispatched = true,
        name,
        onChange,
        options,
        setCreateOrUpdate,
        storeKey,
        value
    } = props;

    // Set up the method used to update the redux store
    const dispatch = useAppDispatch()

    /**
     * A function that runs when the user changes to search based on creation or update user. It is run by the radio
     * button onChange event.
     * @param e - The event that triggered the function.
     */
    const handleUpdateOrCreateChange = (e: React.ChangeEvent<HTMLInputElement>): void => {

        dispatch(setCreateOrUpdate({storeKey, createOrUpdate: e.target.id, id, name}))
    }

    return (
        <React.Fragment>
            <LabelLayout {...props}>
                <div className={"d-flex align-items-center"}>

                    <Form.Check checked={createOrUpdate === "CREATE"}
                                className={"me-3"}
                                id={"CREATE"}
                                inline
                                label={<span className={"text-nowrap"}>Created by</span>}
                                onChange={handleUpdateOrCreateChange}
                                type={"radio"}
                    />

                    <Form.Check checked={createOrUpdate === "UPDATE"}
                                id={"UPDATE"}
                                inline
                                label={<span className={"text-nowrap"}>Updated by</span>}
                                onChange={handleUpdateOrCreateChange}
                                type={"radio"}
                    />

                    <SelectOption basicType={trac.STRING}
                                  className={"w-100"}
                                  id={id}
                                  isClearable={false}
                                  isDisabled={isDisabled}
                                  isDispatched={isDispatched}
                                  mustValidate={false}
                                  onChange={onChange}
                                  options={options}
                                  showValidationMessage={false}
                                  storeKey={storeKey}
                                  validationChecked={false}
                                  validateOnMount={false}
                                  value={value}
                    />
                </div>
            </LabelLayout>
        </React.Fragment>
    )
};

// Some props are passed directly through to the LabelLayout component and not listed below
SelectUserInner.propTypes = {

    createOrUpdate: PropTypes.oneOf(['CREATE', 'UPDATE']).isRequired,
    id: PropTypes.oneOfType([PropTypes.number, PropTypes.string]),
    isDisabled: PropTypes.bool,
    isDispatched: PropTypes.bool,
    name: PropTypes.oneOfType([PropTypes.number, PropTypes.string]),
    onChange: PropTypes.func.isRequired,
    options: PropTypes.array.isRequired,
    setCreateOrUpdate: PropTypes.func.isRequired,
    storeKey: PropTypes.string,
    value: PropTypes.object.isRequired
};

export const SelectUser = memo(SelectUserInner);
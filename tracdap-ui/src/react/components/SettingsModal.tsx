/**
 * A component that shows a modal top right in the screen that allows the user to
 * set various application settings. These are stored in the {@link applicationStore}.
 *
 * @module SettingsModal
 * @category Component
 */

import {Button} from "./Button";
import Col from "react-bootstrap/Col";
import {ConfirmButton} from "./ConfirmButton";
import {convertDateObjectToIsoString} from "../utils/utils_general";
import {General} from "../../config/config_general";
import {Icon} from "./Icon";
import {isOption} from "../utils/utils_trac_type_chckers";
import Modal from "react-bootstrap/Modal";
import type {Option, SelectDatePayload, SelectOptionPayload} from "../../types/types_general";
import PropTypes from "prop-types";
import React, {useCallback, useMemo, useState} from "react";
import Row from "react-bootstrap/Row";
import {SelectDate} from "./SelectDate";
import {SelectOption} from "./SelectOption";
import {setLanguageSetting, setSearchAsOf, setTenantSetting, setThemeSetting} from "../store/applicationStore";
import {tracdap as trac} from "@finos/tracdap-web-api";
import {useAppDispatch, useAppSelector} from "../../types/types_hooks";

// Get some config arrays that list the options to show in the modal.
const {themeOptions, languageOptions} = General.options;

// Some Bootstrap grid layouts
const lgGrid = {span: 8, offset: 2};
const mdGrid = {span: 10, offset: 1};
const xsGrid = {span: 12, offset: 0};

/**
 * An interface for the props of the SettingsModal component.
 */
export interface Props {

    /**
     * Whether to show the modal or not.
     */
    show: boolean
    /**
     * A function that toggles whether the modal is shown or not.
     */
    toggle: React.Dispatch<React.SetStateAction<boolean>>
}

export const SettingsModal = (props: Props) => {

    const {show, toggle} = props

    // Get what we need from the store
    const {
        "trac-theme": theme,
        "trac-tenant": tenant,
        "trac-language": language
    } = useAppSelector(state => state["applicationStore"].cookies)

    const {searchAsOf} = useAppSelector(state => state["applicationStore"].tracApi)
    const {tenantOptions} = useAppSelector(state => state["applicationStore"])

    // Now we want to create a local state version of the options and have them update the application when the user saves the
    // choices. We do this for two reasons, first you don't want the app updating in the background as the user makes changes,
    // second, if the user changes the tenant or the search as of date we need to warn the user that they will reset the
    // application. When changing the search as of then the date and time are counted as different changes, so you don't
    // want a warning popping up with each change nor do you want it resetting after every change.
    const [localOptions, setLocalOptions] = useState<{ language: typeof language, searchAsOf: undefined | string, tenant: typeof tenant, theme: typeof theme }>({
        tenant: tenant,
        theme: theme,
        language: language,
        searchAsOf: searchAsOf?.isoDatetime || undefined
    })

    // Set up the method used to update the redux store
    const dispatch = useAppDispatch()

    // Get the option corresponding to the selected value, we don't store the option in the store we store the cookie
    // string and then look it up here
    const selectedThemeOption = useMemo(() => themeOptions.find(option => option.value === localOptions.theme), [localOptions.theme])
    const selectedLanguageOption = useMemo(() => languageOptions.find(option => option.value === localOptions.language), [localOptions.language])
    const selectedTenantOption = useMemo(() => tenantOptions.find(option => option.value === localOptions.tenant), [localOptions.tenant, tenantOptions])

    /**
     * A function that handles changing the local state version of the options.
     * @param payload - The payload from the {@link SelectOption} or {@link SelectValue} component when the value is changed.
     */
    const handleLocalOptionChange = useCallback((payload: SelectOptionPayload<Option<string>, false> | SelectDatePayload): void => {

        const {name, value} = payload

        if (name === "trac-language" && isOption(value)) {

            setLocalOptions(prevState => ({...prevState, language: value.value}))

        } else if (name === "trac-tenant" && isOption(value)) {

            setLocalOptions(prevState => ({...prevState, tenant: value.value}))

        } else if (name === "trac-theme" && isOption(value)) {

            setLocalOptions(prevState => ({...prevState, theme: value.value}))

        } else if (name === "search-as-of" && (typeof value === "string" || value === null)) {

            setLocalOptions(prevState => ({...prevState, searchAsOf: value || undefined}))
        }
    }, [])

    /**
     * A function that handles updating the Redux store with the new values, this runs when the user
     * clicks save and takes the local versions of the options and saves them to the store.
     */
    const handleStoreOptionChange = (): void => {

        if (localOptions.language !== language) {

            dispatch(setLanguageSetting(localOptions.language))

        } else if (localOptions.tenant !== tenant && typeof localOptions.tenant === "string") {

            dispatch(setTenantSetting(localOptions.tenant))

        } else if (localOptions.theme !== theme) {

            dispatch(setThemeSetting(localOptions.theme))

        } else if (localOptions.searchAsOf !== searchAsOf) {

            dispatch(setSearchAsOf(localOptions.searchAsOf))
        }
        // Close the modal
        toggle(false)
    }

    /**
     * A function that handles resetting the local options to the same as the Redux store if they exit without saving.
     */
    const handleRestoreOptions = (): void => {
        
        if (localOptions.language !== language) {

            setLocalOptions(prevState => ({...prevState, language: language}))

        } else if (localOptions.tenant !== tenant && typeof localOptions.tenant === "string" && typeof tenant === "string") {

            setLocalOptions(prevState => ({...prevState, tenant: tenant}))

        } else if (localOptions.theme !== theme) {

            setLocalOptions(prevState => ({...prevState, theme: theme}))

        } else if (localOptions.searchAsOf !== searchAsOf) {

            setLocalOptions(prevState => ({...prevState, searchAsOf: searchAsOf?.isoDatetime || undefined}))
        }
    }

    return (

        // dialogClassName adds a class to make the modal appear top right
        <Modal show={show} onExited={handleRestoreOptions} onHide={() => toggle(false)} dialogClassName="modal-dialog-top-right">

            <Modal.Header closeButton>
                <Modal.Title>
                    User Settings
                </Modal.Title>
            </Modal.Header>

            <Modal.Body className={"mx-5"}>

                <Row>
                    <Col className={"mt-1"} xs={xsGrid} md={mdGrid} lg={lgGrid}>

                        {/*If you validate on mount then you will reset the store too*/}
                        <SelectOption basicType={trac.STRING}
                                      isDispatched={false}
                                      labelText={"Tenant:"}
                                      options={tenantOptions}
                                      onChange={handleLocalOptionChange}
                                      mustValidate={false}
                                      name={"trac-tenant"}
                                      value={selectedTenantOption || null}
                                      validateOnMount={false}
                                      showValidationMessage={true}
                                      validationChecked={true}
                        />
                    </Col>

                    <Col xs={xsGrid} md={mdGrid} lg={lgGrid}>

                        {/* If you validate on mount then you will reset the store too*/}
                        <SelectDate basicType={trac.DATETIME}
                                    formatCode={"DATETIME"}
                                    isClearable={true}
                                    isDispatched={false}
                                    labelText={"Time travel:"}
                                    onChange={handleLocalOptionChange}
                                    mustValidate={false}
                                    maximumValue={convertDateObjectToIsoString(new Date(), "datetimeIso")}
                                    name={"search-as-of"}
                                    placeHolderText={"Time travel is not set"}
                                    showValidationMessage={true}
                                    tooltip={"Setting this will move the application into the past, working as if it was the selected date and time"}
                                    value={localOptions.searchAsOf ? localOptions.searchAsOf : null}
                                    validateOnMount={false}
                                    validationChecked={false}
                        />
                    </Col>

                    <Col xs={xsGrid} md={mdGrid} lg={lgGrid}>
                        <SelectOption basicType={trac.STRING}
                                      isDispatched={false}
                                      labelText={"Theme:"}
                                      options={themeOptions}
                                      onChange={handleLocalOptionChange}
                                      mustValidate={true}
                                      name={"trac-theme"}
                                      value={selectedThemeOption}
                                      validateOnMount={true}
                                      showValidationMessage={true}
                                      validationChecked={true}
                        />
                    </Col>
                    <Col className={"my-1"} xs={xsGrid} md={mdGrid} lg={lgGrid}>
                        <SelectOption basicType={trac.STRING}
                                      isDispatched={false}
                                      labelText={"Language:"}
                                      options={languageOptions}
                                      onChange={handleLocalOptionChange}
                                      mustValidate={true}
                                      name={"trac-language"}
                                      value={selectedLanguageOption}
                                      validateOnMount={true}
                                      showValidationMessage={true}
                                      validationChecked={true}
                        />
                    </Col>
                </Row>
            </Modal.Body>

            <Modal.Footer>

                <Button ariaLabel={"Close settings menu"}
                        variant={"secondary"}
                        onClick={toggle}
                        isDispatched={false}
                >
                    Close
                </Button>

                {/* If the values of tenant and searchAsOf have not changed then we do not have to warn about resetting the app*/}
                <ConfirmButton ariaLabel={"Save user settings changes"}
                               description={`${tenant !== localOptions.tenant ? "Changing tenant will clear the application and load the new tenant.": ""} ${searchAsOf?.isoDatetime === undefined && localOptions.searchAsOf !== undefined ? "Using time travel mode will move the application into the past, working as if it was the selected date and time. Some functionality will be limited." : ""}. Do you want to continue?`}
                               ignore={Boolean(!(searchAsOf?.isoDatetime === undefined && localOptions.searchAsOf !== undefined) && tenant === localOptions.tenant)}
                               onClick={handleStoreOptionChange}
                               variant={"info"}
                >
                    <Icon ariaLabel={false}
                          icon={'bi-save me-2'}
                    />
                    Save
                </ConfirmButton>

            </Modal.Footer>
        </Modal>
    )
};

SettingsModal.propTypes = {

    show: PropTypes.bool.isRequired,
    toggle: PropTypes.func.isRequired
};
/**
 * A component that shows login widget for authenticating with GitHub.
 *
 * @module GitHubLogin
 * @category Component
 */

import {Alert} from "../../Alert";
import {Button} from "../../Button";
import Col from "react-bootstrap/Col";
import GitHubIcon from "../../../../images/GitHub-Mark-64px.png";
import {gitLogin, setLoginDetails, toggleValidationMessages, type UploadFromGitHubStoreState} from "../store/uploadFromGitHubStore";
import PropTypes from "prop-types";
import React from "react";
import Row from "react-bootstrap/Row";
import {SelectValue} from "../../SelectValue";
import {tracdap as trac} from "@finos/tracdap-web-api";
import {useAppDispatch, useAppSelector} from "../../../../types/types_hooks";

// Some Bootstrap grid layouts
const mdGrid = {span: 8, offset: 2}
const lgGrid = {span: 6, offset: 3}
const xlGrid = {span: 4, offset: 4}

/**
 * An interface for the props of the GitHubLogin component.
 */
export interface Props {

    /**
     * The key in the UploadFromGitHubStore to get the state for this component
     */
    storeKey: keyof UploadFromGitHubStoreState["uses"]
}

export const GitHubLogin = (props: Props) => {

    const {storeKey} = props

    // Get what we need from the store
    const {authorisation, user} = useAppSelector(state => state["uploadFromGitHubStore"].uses[storeKey])

    // Set up the method used to update the redux store
    const dispatch = useAppDispatch()

    /**
     * A function that runs when the user clicks the login button to authenticate with GitHub.
     */
    const login = (): void => {

        // Update the store to say if there is a validation error then show the message
        dispatch(toggleValidationMessages({storeKey, value: true}))

        // Only log in if the name and token inputs are validated e.g. if not empty
        if (authorisation.userName.isValid === true && authorisation.token.isValid === true) {
            dispatch(gitLogin({storeKey}))
        }
    }

    return (

        <React.Fragment>
            {/*Only show the login in no successful login has occurred*/}
            {user.status !== "succeeded" &&

                <Row className={"mt-5"}>
                    <Col className={"py-5 px-5 border background-secondary"}
                         xs={12} md={mdGrid} lg={lgGrid} xl={xlGrid}>

                        <img height={64}
                             className={"d-flex mx-auto mb-4"}
                             src={GitHubIcon}
                             alt={"GitHub"}
                        />

                        <SelectValue basicType={trac.STRING}
                            // Make the input have a white background
                                     className={"bg-input-body-background"}
                                     id={"userName"}
                                     labelPosition={"top"}
                                     labelText={"User name:"}
                                     minimumValue={4}
                                     mustValidate={true}
                                     name={storeKey}
                                     onChange={setLoginDetails}
                                     onEnterKeyPress={login}
                                     showValidationMessage={true}
                                     value={authorisation.userName.value}
                                     validationChecked={authorisation.validationChecked}
                                     validateOnMount={false}
                        />

                        <SelectValue basicType={trac.STRING}
                            // Make the input have a white background
                                     className={"mt-2 bg-input-body-background"}
                                     id={"token"}
                                     labelPosition={"top"}
                                     labelText={"Personal Access Token (PAT):"}
                                     minimumValue={40}
                                     mustValidate={true}
                                     name={storeKey}
                                     onChange={setLoginDetails}
                                     onEnterKeyPress={login}
                                     showValidationMessage={true}
                                     specialType={"PASSWORD"}
                                     tooltip={"In order to login you must first have a GitHub account and also a Personal Access Token (PAT) set up to use for authentication. Information about creating a token can be found in the GitHub documentation."}
                                     value={authorisation.token.value}
                                     validationChecked={authorisation.validationChecked}
                                     validateOnMount={false}
                        />

                        <Button ariaLabel={"Login"}
                                className={"float-end min-width-px-125 mt-3"}
                                isDispatched={false}
                                loading={Boolean(user.status === "pending")}
                                onClick={login}
                        >
                            Login
                        </Button>

                        {user.message &&
                            <Alert variant={"danger"} className={"mt-3"}>
                                {user.message}
                            </Alert>
                        }
                    </Col>
                </Row>
            }
        </React.Fragment>
    )
};

GitHubLogin.propTypes = {

  storeKey: PropTypes.string.isRequired
};
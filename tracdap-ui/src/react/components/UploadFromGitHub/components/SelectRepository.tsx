/**
 * A component that shows a list of repositories for the user to select to load from.
 *
 * @module SelectRepository
 * @category Component
 */

import {Button} from "../../Button";
import Col from "react-bootstrap/Col";
import {getGitRepositories, getRepository, setRepository, type UploadFromGitHubStoreState} from "../store/uploadFromGitHubStore";
import {HeaderTitle} from "../../HeaderTitle";
import {Icon} from "../../Icon";
import PropTypes from "prop-types";
import React, {useEffect, useRef, useState} from "react";
import Row from "react-bootstrap/Row";
import {SelectOption} from "../../SelectOption";
import {TextBlock} from "../../TextBlock";
import {tracdap as trac} from "@finos/tracdap-web-api";
import {useAppDispatch, useAppSelector} from "../../../../types/types_hooks";

/**
 * An interface for the props of the SelectRepository component.
 */
export interface Props {

    /**
     * The key in the UploadFromGitHubStore to get the state for this component
     */
    storeKey: keyof UploadFromGitHubStoreState["uses"]
    /**
     * The type of file being selected, it is used in messages to the user.
     */
    uploadType: "model" | "schema"
}

export const SelectRepository = (props: Props) => {

    const {storeKey, uploadType} = props

    // Get what we need from the store
    const user = useAppSelector(state => state["uploadFromGitHubStore"].uses[storeKey].user)
    const repositories = useAppSelector(state => state["uploadFromGitHubStore"].uses[storeKey].repositories)

    // We are going to allow the user to toggle between showing and hiding the disabled repos as there could be loads
    // and only a few that are available
    const [hideDisabledOptions, setHideDisabledOptions] = useState(true)

    // Set up the method used to update the redux store
    const dispatch = useAppDispatch()

    /**
     * A function that runs when the user clicks to toggle the repository option filtering to hide/show disabled
     * repositories.
     */
    function toggleHideDisabledOptions(): void {

        setHideDisabledOptions((prevState) => !prevState)
    }
    
    /**
     * Awesomeness. A hook that runs every render except when the component mounts. This starts with this ref.
     */
    const componentIsMounting = useRef(true)

    /**
     * A hook that runs when the selected repository changes, this goes and gets the information about the repository.
     * Note that we use componentIsMounting to prevent it from running when the components mounts, this is so we use
     * the values in the store rather than getting all the information again. We also do this because some functions
     * that get called in this component reset parts of the store, so if you allow them to run when the component 
     * mounts you end up wiping parts of the state.
     */
    useEffect(() => {

        // Strictly only ever run this after the component mounts
        if (!componentIsMounting.current) {
            // The selected option is not really needed, but we want to trigger this
            // when the option changes
            if (repositories.selectedOption) dispatch(getRepository({storeKey}))
        } else {
           componentIsMounting.current = false
        }

    }, [dispatch, repositories.selectedOption, storeKey])

    return (

        <React.Fragment>
            {/*Show once logged in*/}
            {user.status === "succeeded" &&

                <React.Fragment>
                    <HeaderTitle type={"h3"} text={"Select your repository"}/>

                    <TextBlock>
                        Below is a list of your Git repositories. You are only allowed to upload {uploadType}s from
                        repositories registered in both the application config and the TRAC config, if you need to add a
                        new repository then contact your support team. By default unauthorised repos are hidden from the
                        list of options but the buttons below can be used to see the full list downloaded.
                    </TextBlock>

                    <Row>
                        <Col xs={9} md={7} lg={6} xl={5}>
                            <SelectOption basicType={trac.STRING}
                                          hideDisabledOptions={hideDisabledOptions}
                                          isLoading={Boolean(repositories.status === "pending")}
                                          name={storeKey}
                                          onChange={setRepository}
                                          options={repositories.options}
                                          placeHolderText={Boolean(repositories.status == "pending") ? "Loading..." : undefined}
                                          validateOnMount={false}
                                          value={repositories.selectedOption}
                            />
                        </Col>

                        <Col xs={3} md={5} lg={6} xl={7} className={"my-auto"}>
                            <Button ariaLabel={"Refresh repositories"}
                                    className={"m-0 p-0 fs-4"}
                                    id={"refresh"}
                                    name={storeKey}
                                    onClick={getGitRepositories}
                                    variant={"link"}
                            >
                                <Icon ariaLabel={false}
                                      icon={"bi-arrow-clockwise"}
                                      tooltip={"Refresh repositories"}
                                />
                            </Button>

                            <Button ariaLabel={"Toggle disabled repositories"}
                                    className={"my-0 me-0 ms-3 p-0 fs-4"}
                                    isDispatched={false}
                                    onClick={toggleHideDisabledOptions}

                                    variant={"link"}
                            >
                                <Icon ariaLabel={false}
                                      icon={!hideDisabledOptions ? "bi-list-task" : "bi-funnel"}
                                      tooltip={hideDisabledOptions ? "Show disabled repositories" : "Hide disabled repositories"}
                                />
                            </Button>
                        </Col>
                    </Row>

                </React.Fragment>
            }
        </React.Fragment>
    )
};

SelectRepository.propTypes = {

    storeKey: PropTypes.string.isRequired
};
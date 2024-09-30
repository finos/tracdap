/**
 * A component that shows information about the selected commit.
 *
 * @module CommitInformation
 * @category Component
 */

import Col from "react-bootstrap/Col";
import {Icon} from "../../Icon";
import {isGitHubCommit} from "../../../utils/utils_trac_type_chckers";
import PropTypes from "prop-types";
import React from "react";
import Row from "react-bootstrap/Row";
import Table from "react-bootstrap/Table";
import {type UploadFromGitHubStoreState} from "../store/uploadFromGitHubStore";
import {useAppSelector} from "../../../../types/types_hooks";

/**
 * An interface for the props of the CommitInformation component.
 */
export interface Props {

    /**
     * The key in the UploadFromGitHubStore to get the state for this component
     */
    storeKey: keyof UploadFromGitHubStoreState["uses"]
}

export const CommitInformation = (props: Props) => {

    const {storeKey} = props

    // Get what we need from the store
    const {commit} = useAppSelector(state => state["uploadFromGitHubStore"].uses[storeKey].branch)

    return (

        <React.Fragment>

            {/*When loading from a release or a tag the committer property will not exist, it is only present on a commit*/}
            {/*so we have type guard check for the property, also we check that the message is not empty */}
            {isGitHubCommit(commit.selectedOption?.details.commit) && commit.selectedOption?.details.commit.commit.message &&
                <Row>
                    {/*These columns match the select commit widths so the information appears in line with the select.*/}
                    <Col xs={9} md={7} lg={6} xl={5} className={"mt-3 mb-3 d-flex"}>
                        <div>
                            <Icon ariaLabel={"Commit message"}
                                  className={"me-3"}
                                  icon={"bi-chat-left-dots"}
                                  size={"2rem"}
                                  tooltip={"Commit message"}
                            />
                        </div>
                        <div className={"mt-1"}>&#8220;{commit.selectedOption.details.commit.commit.message}&#8221;</div>
                    </Col>
                </Row>
            }
        </React.Fragment>
    )
};

CommitInformation.propTypes = {

    storeKey: PropTypes.string.isRequired
};
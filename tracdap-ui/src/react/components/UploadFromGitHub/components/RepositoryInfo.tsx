/**
 * A component that shows the selected repository's info.
 *
 * @module RepositoryInfo
 * @category Component
 */

import {A} from "../../A";
import {convertIsoDateStringToFormatCode} from "../../../utils/utils_formats";
import {Icon} from "../../Icon";
import {Loading} from "../../Loading";
import React from "react";
import {RepoIcon} from "./RepoIcon";
import {useAppSelector} from "../../../../types/types_hooks";
import {setLanguageOrFileIcon, sOrNot} from "../../../utils/utils_general";
import {type UploadFromGitHubStoreState} from "../store/uploadFromGitHubStore";
import PropTypes from "prop-types";

/**
 * An interface for the props of the RepositoryInfo component.
 */
export interface Props {

    /**
     * The key in the UploadFromGitHubStore to get the state for this component
     */
    storeKey: keyof UploadFromGitHubStoreState["uses"]
}

export const RepositoryInfo = (props: Props) => {

    const {storeKey} = props

    // Get what we need from the store
    const {
        branches,
        contributors,
        details,
        pulls,
        status
    } = useAppSelector(state => state["uploadFromGitHubStore"].uses[storeKey].repository)

    return (

        <React.Fragment>

            {/*Only show the loader icon during the first login*/}
            {status === "pending" && details == null &&
                <Loading text={"Please wait..."} className={"my-5"}/>
            }

            {details != null &&

                <React.Fragment>
                    <div
                        className="git git-header fs-1 background-secondary mt-6 d-flex align-items-center justify-content-between px-3 py-3 py-lg-1">
                        <div>
                            <A className={"git git-title fw-lighter git-link"}
                               href={details.owner.html_url}
                            >
                                {details.owner.login}
                            </A>
                            <span className={"git git-title fw-lighter"}>/</span>
                            <A className={"git git-title fw-bolder git-link"}
                               href={details.html_url}
                            >
                                {details.name}
                            </A>
                        </div>

                        <div className={"d-none d-md-flex"}>
                            <RepoIcon count={contributors.length}
                                      href={`${details.html_url}/contributors`}
                                      icon={"bi-people"}
                                      label={`Contributor${sOrNot(contributors)}`}
                            />
                            <RepoIcon count={branches.options.length}
                                      href={`${details.html_url}/branches`}
                                      icon={"bi-diagram-2"}
                                      label={`Branch${sOrNot(branches.options, "es")}`}
                            />
                            {/*The className below makes the icon a little smaller than the others*/}
                            <RepoIcon className={"fs-2"}
                                      count={pulls.length}
                                      href={`${details.html_url}/pulls`}
                                      icon={"bi-box-arrow-up"}
                                      label={`Pull request${sOrNot(pulls)}`}
                            />

                        </div>
                    </div>

                    <div className={"d-flex justify-content-between  align-items-center  git git-body py-3 px-3 "}>
                        <div className={"git-repo-description text-tertiary spaced-text fs-11 fw-lighter"}>
                            {details.description}
                        </div>
                        <div className={"fs-1 text-secondary"}>
                            <Icon ariaLabel={"Repository code language"}
                                  className={" ms-3"}
                                  icon={setLanguageOrFileIcon(details.language)}
                                  placement={"left"}
                                  tooltip={details.language || "Language not set"}
                            />
                        </div>
                    </div>

                    {details.pushed_at &&

                        <div className={"git git-footer background-secondary px-3 py-1 mb-6"}>

                            Latest commit to the{' '}
                            <strong>{details.default_branch}</strong> branch
                            on {convertIsoDateStringToFormatCode(details.pushed_at, "DATETIME")}

                        </div>
                    }

                </React.Fragment>
            }
        </React.Fragment>
    )
};

RepositoryInfo.propTypes = {

  storeKey: PropTypes.string.isRequired
};
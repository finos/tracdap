import {BottomMenu} from "./react/components/BottomMenu";
import {BrowserRouter as Router} from "react-router-dom";
import Container from "react-bootstrap/Container";
import ErrorBoundary from "./react/components/ErrorBoundary";
import {General} from "./config/config_general";
import {Loading} from "./react/components/Loading";
import {OnLoad} from "./react/components/OnLoad";
import React, {Suspense, useEffect, useRef} from "react";
import {ReAuthenticationModal} from "./react/components/ReAuthenticationModal";
import {Scenes} from "./react/components/Scenes";
import {ScrollToTop} from "./react/components/ScrollToTop";
import {SideMenu} from "./react/components/SideMenu";
import {toast, ToastContainer} from "react-toastify";
import {toggleSideMenu} from "./react/store/applicationStore";
import {TopBanner} from "./react/components/TopBanner";
import {TopMenu} from "./react/components/TopMenu";
import {useAppDispatch} from "./types/types_hooks";

// Import the application scss. Note that in development builds the scss file is part of the
// bundle as this is quicker to devlop on. In production builds a webpack plugin (MiniCssExtractPlugin)
// is used to strip out the scss and move it to an external css file.
import './css/App.scss';
import {ExpiryModal} from "./react/components/ExpiryModal";

export function App() {

    // Set up the method used to update the redux store
    const dispatch = useAppDispatch()

    /**
     * A reference to the side menu in the DOM.
     */
    const wrapperRef = useRef<HTMLInputElement>(null);

    /**
     * A hook that adds an event listener after the application mounts and removes it when
     * the application is unmounted. The array argument says never run this on any render other
     * than the first. It also gets data from TRAC needed to make the application run.
     */
    useEffect(() => {

        /**
         * A function that runs when the user clicks outside the side menu that will cause
         * it to be hidden.
         * @param e (event) A event in the DOM.
         */
        const handleClickOutsideSideMenu = (e: MouseEvent) => {

            const {current} = wrapperRef;

            // The next bit looks a bit complicated, but it is here for a reason. We only want to run the toggleSideMenu
            // action when the side menu is showing however if we load the show value from the store in order to use this
            // to work out if it is showing then each time this value is changed the whole app will re-render. So instead
            // we use pure javascript to work out if the side menu is inside or outside the viewport. We get fewer
            // dispatches as a result.

            // Is the user clicking outside the menu
            if (current && e && !current.contains(e.target as HTMLElement)) {

                // Get the side menu
                let sideMenu = document.getElementById('side-menu');

                if (sideMenu) {
                    // Get its position in the viewport
                    let bounding = sideMenu.getBoundingClientRect();

                    // Is the side menu showing, if so hide it
                    if (bounding.top >= 0 && bounding.left >= 0) {
                        dispatch(toggleSideMenu(false))
                    }
                }
            }
        }

        document.addEventListener('mousedown', handleClickOutsideSideMenu)

        return () => {
            document.removeEventListener('mousedown', handleClickOutsideSideMenu)
        }

    }, [dispatch])

    return (
        // Parent node can not be a div as we inherit flex css to the header, main and footer
        <React.Fragment>
            <Router basename={General.publicPath}>
                <header>
                    {/*Run some things on the application loading*/}
                    <OnLoad/>
                    {/*Scroll to the top when we move to a new page*/}
                    <ScrollToTop/>
                    <div ref={wrapperRef}>
                        <TopMenu/>
                        <SideMenu/>
                    </div>
                    <TopBanner/>
                </header>
                <main>
                    <Container>
                        <Suspense fallback={<Loading className={"my-4 py-4"} text={false}/>}>
                            <ErrorBoundary>
                                <Scenes/>
                            </ErrorBoundary>
                        </Suspense>
                    </Container>
                </main>
                <ReAuthenticationModal/>
            </Router>
            <footer>
                <BottomMenu/>
            </footer>
            {/*ToastContainer is the pop-up message widget*/}
            <ToastContainer position={toast.POSITION.BOTTOM_RIGHT} autoClose={false} draggablePercent={60}/>
            {/*This is a special div, do not remove. This is used with the SelectOption component. There are some*/}
            {/*instances where we need to attach the menu that opens when the user goes to select an option*/}
            {/*to a separate div in the UI, this is usually when the SelectOption component is shown inside*/}
            {/*a table or some such element that would otherwise clip the menu. The SelectOption component has a */}
            {/*useMenuPortalTarget prop that is used to set the div below as where the menu is mounted so that it is */}
            {/*outside the table.  It is the Table component that uses this prop for example.*/}
            <div id={"custom-react-select"}/>
            {/*Disable app if demo period has expired*/}
            <ExpiryModal/>
        </React.Fragment>
    );
}

export default App;

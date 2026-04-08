from auth import check_password
import streamlit as st


def main() -> None:
    if not check_password():
        st.stop()


if __name__ == "__main__":
    main()
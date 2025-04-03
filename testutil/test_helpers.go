package testutil

import (
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
)

func AssertLessThanEqual(t *testing.T, a, b uint64) {
	t.Helper()
	if a > b {
		t.Errorf("Expected %d <= %d", a, b)
	}
}

// Helper function that would be needed for tu.AssertTrue
func AssertTrue(t *testing.T, condition bool) {
	t.Helper()
	if !condition {
		t.Errorf("Expected condition to be true")
	}
}

func AssertEqual(t testing.TB, expected, actual interface{}, msgAndArgs ...interface{}) {
	t.Helper()
	if !reflect.DeepEqual(expected, actual) {
		t.Errorf("Not equal: \nexpected: %v\nactual  : %v\n%s", expected, actual, FormatMsgAndArgs(msgAndArgs...))
	}
}

func AssertNoError(t testing.TB, err error, msgAndArgs ...interface{}) {
	t.Helper()
	if err != nil {
		t.Errorf("Unexpected error: %v\n%s", err, FormatMsgAndArgs(msgAndArgs...))
	}
}

func AssertError(t testing.TB, err error, msgAndArgs ...interface{}) {
	t.Helper()
	if err == nil {
		t.Errorf("Expected an error but got nil\n%s", FormatMsgAndArgs(msgAndArgs...))
	}
}

func AssertErrorIs(t testing.TB, err, target error, msgAndArgs ...interface{}) {
	t.Helper()
	if !errors.Is(err, target) {
		t.Errorf("Expected error to be %v but got %v\n%s", target, err, FormatMsgAndArgs(msgAndArgs...))
	}
}

func AssertLen(t testing.TB, object interface{}, length int, msgAndArgs ...interface{}) {
	t.Helper()
	v := reflect.ValueOf(object)
	if v.Len() != length {
		t.Errorf("Length not equal: \nexpected: %d\nactual  : %d\n%s", length, v.Len(), FormatMsgAndArgs(msgAndArgs...))
	}
}

func AssertEmpty(t testing.TB, object interface{}, msgAndArgs ...interface{}) {
	t.Helper()
	v := reflect.ValueOf(object)
	if v.Len() != 0 {
		t.Errorf("Expected empty but got length %d\n%s", v.Len(), FormatMsgAndArgs(msgAndArgs...))
	}
}

func AssertNotNil(t testing.TB, object interface{}, msgAndArgs ...interface{}) {
	t.Helper()
	if object == nil {
		t.Errorf("Expected not nil but got nil\n%s", FormatMsgAndArgs(msgAndArgs...))
		return
	}

	v := reflect.ValueOf(object)
	if v.Kind() == reflect.Ptr && v.IsNil() {
		t.Errorf("Expected not nil but got nil pointer\n%s", FormatMsgAndArgs(msgAndArgs...))
	}
}

// Format message and arguments for error output
func FormatMsgAndArgs(msgAndArgs ...interface{}) string {
	if len(msgAndArgs) == 0 {
		return ""
	}
	if len(msgAndArgs) == 1 {
		return fmt.Sprintf("\nMessage: %s", msgAndArgs[0])
	}
	return fmt.Sprintf("\nMessage: %s", fmt.Sprintf(msgAndArgs[0].(string), msgAndArgs[1:]...))
}

// Custom require helpers that fail the test immediately
func RequireNoError(t testing.TB, err error, msgAndArgs ...interface{}) {
	t.Helper()
	if err != nil {
		t.Fatalf("Required no error but got: %v\n%s", err, FormatMsgAndArgs(msgAndArgs...))
	}
}

func RequireNotNil(t testing.TB, object interface{}, msgAndArgs ...interface{}) {
	t.Helper()
	if object == nil {
		t.Fatalf("Required not nil but got nil\n%s", FormatMsgAndArgs(msgAndArgs...))
		return
	}

	v := reflect.ValueOf(object)
	if v.Kind() == reflect.Ptr && v.IsNil() {
		t.Fatalf("Required not nil but got nil pointer\n%s", FormatMsgAndArgs(msgAndArgs...))
	}
}

func AssertContains(t testing.TB, s, substr string, msgAndArgs ...interface{}) {
	t.Helper()
	if !strings.Contains(s, substr) {
		t.Errorf("Expected string to contain substring:\nstring: %q\nsubstring: %q\n%s", s, substr, FormatMsgAndArgs(msgAndArgs...))
	}
}

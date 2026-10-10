//go:build integration

package main

import "testing"

func TestIndependentLinuxWorkersRecover27ChildSweep(t *testing.T) {
	testWorkerDaemonsRecover27ChildSweep(t, newIndependentSweepDaemonFixture(t))
}

func TestIndependentLinuxWorkersRejectDelayedResult(t *testing.T) {
	testWorkerDaemonsRejectDelayedSweepResult(t, newIndependentSweepDaemonFixture(t))
}

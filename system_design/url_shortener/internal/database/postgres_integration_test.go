package database

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestPostgresIntegration(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "postgres integration")
}

var _ = Describe("postgres integration tests", func() {
	var pgStore *PGDataStore
	var conn *pgx.Conn
	var connErr error

	BeforeEach(func(ctx SpecContext) {
		conn, connErr = pgx.Connect(ctx, "postgres://postgres:password@localhost:5432/postgres")
		Expect(connErr).ToNot(HaveOccurred())
		_, connErr = conn.Exec(ctx, CreateSchemaSqlString)
		Expect(connErr).ToNot(HaveOccurred())

		pgStore = NewPGDataStore(conn)
	})
	AfterEach(func(ctx SpecContext) {
		conn.Exec(ctx, DropSchemaSqlString)
		conn.Close(ctx)
	})
	When("creating a record", func() {
		It("works", func(ctx SpecContext) {
			// TODO
			_, err := pgStore.CreateShortenedRecord(ctx, "encoded", "target")
			Expect(err).To(BeNil())
		})
	})
	When("you get a short record", func() {
		It("works", func(ctx context.Context) {
			shortened, err := pgStore.CreateShortenedRecord(ctx, "encoded", "target")
			Expect(err).To(BeNil())

			obs, err := pgStore.Get(ctx, shortened.Encoded)
			Expect(err).To(BeNil())
			Expect(obs.Id).To(Equal(shortened.Id))
		})
	})
	When("creating a visit", func() {
		It("works", func(ctx context.Context) {
			shortened, err := pgStore.CreateShortenedRecord(ctx, "encoded", "target")
			Expect(err).To(BeNil())

			visit, err := pgStore.CreateVisitRecord(ctx, shortened.Id)
			Expect(err).To(BeNil())
			Expect((*visit).ShortId).To(Equal(shortened.Id))
		})
	})
})

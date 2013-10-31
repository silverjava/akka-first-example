package akka.tutorial.first.java;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.actor.UntypedActor;
import akka.routing.RoundRobinPool;
import scala.concurrent.duration.Duration;

import java.util.concurrent.TimeUnit;

public class Pi {

    static class Calculate {}

    static class Work {
        private final int start;
        private final int nrOfElements;

        public Work(int start, int nrOfElements) {
            this.start = start;
            this.nrOfElements = nrOfElements;
        }

        public int getStart() {
            return start;
        }

        public int getNrOfElements() {
            return nrOfElements;
        }
    }

    static class Result {
        private final double value;

        public Result(double value) {
            this.value = value;
        }

        public double getValue() {
            return value;
        }
    }

    static class PiApproximation {
        private final double pi;
        private final Duration duration;

        public PiApproximation(double pi, Duration duration) {
            this.pi = pi;
            this.duration = duration;
        }

        public double getPi() {
            return pi;
        }

        public Duration getDuration() {
            return duration;
        }
    }

    public static class Worker extends UntypedActor {

        @Override
        public void onReceive(Object message) throws Exception {
            if (message instanceof Work) {
                Work work = (Work) message;
                double result = calculatePiFor(work.getStart(), work.getNrOfElements());
                getSender().tell(new Result(result), getSelf());
            }
        }

        private double calculatePiFor(int start, int nrOfElements) {
            double acc = 0.0;

            for (int i = start * nrOfElements; i < (start + 1) * nrOfElements; i++) {
                acc += 4.0 * (1 - (i % 2) * 2) / (2 * i + 1);
            }

            return acc;
        }
    }

    public static class Master extends UntypedActor {
        private final int nrOfMessages;
        private final int nrOfElements;
        private double pi;

        private int nrOfResult;
        private final long start = System.currentTimeMillis();

        private final ActorRef listener;
        private final ActorRef workerRouter;

        public Master(final int nrOfWorkers, int nrOfMessages, int nrOfElements, ActorRef listener) {
            this.nrOfMessages = nrOfMessages;
            this.nrOfElements = nrOfElements;
            this.listener = listener;

            workerRouter = getContext().actorOf(
                    Props.create(Worker.class).withRouter(new RoundRobinPool(nrOfWorkers)), "workerRouter");
        }

        @Override
        public void onReceive(Object message) throws Exception {
            if (message instanceof Calculate) {
                for (int start = 0; start < nrOfMessages; start++) {
                    workerRouter.tell(new Work(start, nrOfElements), getSelf());
                }
            } else if (message instanceof Result) {
                Result result = (Result) message;
                pi += result.getValue();
                nrOfResult++;

                if (nrOfResult == nrOfMessages) {
                    Duration duration = Duration.create(System.currentTimeMillis() - start, TimeUnit.MILLISECONDS);
                    listener.tell(new PiApproximation(pi, duration), getSelf());

                    getContext().stop(getSelf());
                }
            } else {
                unhandled(message);
            }
        }
    }

    public static class Listener extends UntypedActor {

        @Override
        public void onReceive(Object message) throws Exception {
            if (message instanceof PiApproximation) {
                PiApproximation piApproximation = (PiApproximation) message;

                System.out.println(String.format("\n\tPi approximation: \t\t%s\n\tCalculation time: \t\t%s",
                        piApproximation.getPi(), piApproximation.getDuration()));

                getContext().system().shutdown();
            } else {
                unhandled(message);
            }
        }
    }

    public void calculate(final int nrOfWorkers, final int nrOfMessages, final int nrOfElements) {
        ActorSystem system = ActorSystem.create("PiSystem");

        final ActorRef listener = system.actorOf(Props.create(Listener.class), "listener");

        ActorRef master = system.actorOf(Props.create(Master.class, nrOfWorkers, nrOfMessages, nrOfElements, listener), "master");
        master.tell(new Calculate(), master);
    }

    public static void main(String[] args) {
        Pi pi = new Pi();
        pi.calculate(8, 10000, 10000);
    }
}

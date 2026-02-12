import random
import math
import sys


# CHOOSE THE CENTER OF A CIRCLE OF A POINTS
def genA(x, y, R):
    angle = random.uniform(0, 2 * math.pi)      # choose a random angle offset
    ratio = random.uniform(0.4, 0.7)
    r_A = ratio * R         # choose a random position on the radius of the circle of B points
    x_A = x + r_A * math.cos(angle)
    y_A = y + r_A * math.sin(angle)

    return (x_A, y_A)


# GENERATE A CERTAIN NUMBER OF POINTS ARRANGED IN A CIRCLE WITH A GIVEN CENTER
def circle(num_points, center, label, radius):
    points = []
    for j in range(num_points):
        angle = random.uniform(0, 2 * math.pi)      # choose a random angle offset
        r = radius * math.sqrt(random.uniform(0, 1))    # choose a random position on the radius of the circle we are generating
        x = center[0] + r * math.cos(angle)
        y = center[1] + r * math.sin(angle)
        point = (x, y, label)
        points.append(point)
        print(f"{point[0]},{point[1]},{label}")

    return points


# GENERATE MULTIPLE CIRCULAR CLUSTERS
def generate_circles(N, K):
    points = []
    quadrants = [
        (1, 1),  # I quadrant
        (-1, 1),  # II quadrant
        (-1, -1),  # III quadrant
        (1, -1)  # IV quadrant
    ]

    num_clus_quad = K // 4  # we will generate an equal number of clusters over the four quadrants
    clus_remainder = K - num_clus_quad * 4  # useful to manage the case where K/4 is not an even division

    points_per_cluster = N // K  # we will assign an equal number of points to the clusters
    points_remainder = N - points_per_cluster * K  # useful to manage the case of N/K is not an even division

    radius = 12 / K

    for x, y in quadrants:
        '''
        x and y simply represents the sign of the quadrant
        '''

        # if the number of clusters is not divisible by 4 we assign one more cluster to each quadrant till there are no remainder clusters left
        c = num_clus_quad
        if clus_remainder > 0:
            c += 1
            clus_remainder -= 1

        for i in range(c):
            offset_x = random.randint(4, 20)
            offset_y = random.randint(4, 20)
            center = (x * offset_x * 1.3, y * offset_y * 1.3)  # we decided to multiply the offset by a factor to separate circles even more

            # if the number of points is not divisible by K we assign one more point to each cluster till there are no remainder points left
            n = points_per_cluster
            if points_remainder > 0:
                n += 1
                points_remainder -= 1

            max_A = int(points_per_cluster * 0.4)
            points_A = random.randint(1,
                                      max_A)  # generate a random number in order to establish how many points the circle of points A will contain
            points_B = n - points_A  # subtract the number of points A from the number of points B (remainder included)

            cluster_points_B = circle(points_B, center, 'B', radius)
            points.extend(cluster_points_B)
            cluster_points_A = circle(points_A, genA(center[0], center[1], radius), 'A', radius)
            points.extend(cluster_points_A)

    return points


def main():

    N = sys.argv[1]
    assert N.isdigit(), "N must be an integer"
    N = int(N)

    K = sys.argv[2]
    assert K.isdigit(), "K must be an integer"
    K = int(K)

    assert N//K > 4, "number of clusters is too low with respect to the number of points"

    points = generate_circles(N, K)


if __name__ == "__main__":
    main()

